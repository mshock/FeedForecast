#! /usr/bin/perl -w

use strict;
use DBI;
use Scalar::Util qw(looks_like_number);
use FeedForecast;
# update the status (waiting, recv'd, late) for each exchange
# also, once recv'd mark whether volume or time are close to forecast

my $config = FeedForecast::loadConfig();

# run once flag
my $runonce = $config->runonce();

my $runnet_log = $config->runnet_log();

my $daemon_log = $config->daemon_log();
# percentage of forecast that needs to be reached before feed is marked as complete
my $threshold = $config->comp_thresh();
# adjustment parameters
my $pivot = $config->pivot();
my $delta = $config->delta();

# amount of minutes to allow a feed to be late past forecast
my $to_thresh = $config->late_thresh();
# number of minutes between updates
my $freq = $config->freq(); 



print FeedForecast::currtime() . "\tstarting update daemon...\n";
my $first = 1;

do {
	# run update tasks at frequency
	if (!$first) {
		sleep($freq * 60);
	}
	else {
		$first = 0;
	}
	
	print FeedForecast::currtime() . "\tstarted update task\n";
	
	# need to run both previous and current day
	my $date = FeedForecast::calc_date();
	my ($prevdate, $trash1, $trash2) = FeedForecast::decrement_day($date);
	if (fork()) {
		make_pass($date, 0);
	}
	else {
		make_pass($prevdate, 1);
		exit;
	}
	
	print FeedForecast::currtime() . "\tfinished update task\n";
} while (!$runonce);

# make a daemon pass for given date
sub make_pass {
	my ($date, $prev_flag) = @_;
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");  
		
	my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
	
	
	my $get_cur_count = $ds2->prepare("select distinct exchintcode, count(infocode)  
				from [DataStream2].[dbo].[DS2PrimQtPrc] with (NOLOCK)
				where marketdate = '$date'
				group by exchintcode
				order by 1 ASC");
	
	
	# load hash with exchange forecasts
	
	my $select_result = $nndb->prepare("select [ExchID]
      ,[ExchName]
      ,[InputOffset]
      ,[DayofMonth]
      ,[DayofWeek]
      ,[InputVolume]
      ,[OutputOffset]
      ,[OutputVolume] from NetResults where
										[Date] = '$date'");
	$select_result->execute();
	my %exchhash = ();
	while (my @row = $select_result->fetchrow_array()) {
		my ($id,$name,$timeoffset,$dom,$dow,$vol,$timeoffset2,$vol2) = @row;
		#print "$name,$id,$timeoffset,$dom,$dow,$vol,$timeoffset2,$vol2\n";
		#print "loaded exchange: $name\n";
		# detect bad NN outputs
		my $state = "wait";
		if (!looks_like_number($vol2) || !$vol2) {
			$vol2 = -1;
			$state = "error";
		}
		if (!looks_like_number($timeoffset2) || !$timeoffset2) {
			$timeoffset2 = -1;
			$state = "error";
		}
		# mark as late
		if (server_time_offset($prev_flag) > $timeoffset2 + $to_thresh) {
			$state = "late";
		}
		
		%{$exchhash{$id}} = (
			name => $name,
			dom => $dom,
			dow => $dow,
			to => $timeoffset,
			vol => $vol,
			to2 => $timeoffset2,
			vol2 => $vol2,
			count => 0,
			state => $state,
		);
	}
	$select_result->finish();
	
	$get_cur_count->execute();
	# check to see if the current counts have reached the threshold per exchange
	while (my @count = $get_cur_count->fetchrow_array()) {
		my ($exchid, $count) = @count;
		my $predicted = $exchhash{$exchid}{vol2};
		# check if this exchange exists in hash (might not due to previous errors)
		if (! exists $exchhash{$exchid} || !$predicted) {
			next;
		}
		# check if we have reached the threshold for complete
		$exchhash{$exchid}{count} = $count;
		#print "query result: $exchid $count\n";
		#if ($exchhash{$exchid}{vol2} < 0 || $exchhash{$exchid}{to2} < 0) {
		#	$exchhash{$exchid}{state} = "error";
		#}
		my $ratio = $count / $predicted;
		my $adjusted = $threshold - ($delta - $predicted / ($pivot + $predicted) * $delta);
	#	if ($count >= ($predicted * $threshold)) {
		if ($ratio >= $adjusted) {
			$exchhash{$exchid}{state} = "recv";
		}
		# threshold not yet reached
		#else {
			
		#}
	}
		
	# clear and write new weblog file
	my %prevhash = load_log($date);
	my $email_body = '';
	
	open (OLOG, '>', sprintf($daemon_log,$date));
	foreach my $exchange (sort (keys %exchhash)) {
		 my ($name, $to, $dom, $dow, $vol, $to2, $vol2, $count, $state) = 
		 				($exchhash{$exchange}{name},
						$exchhash{$exchange}{to},
						$exchhash{$exchange}{dom},
						$exchhash{$exchange}{dow},
						$exchhash{$exchange}{vol},
						$exchhash{$exchange}{to2},
						$exchhash{$exchange}{vol2},
						$exchhash{$exchange}{count},
						$exchhash{$exchange}{state});
		
		# check if this exchange even exists (some returned by query do not)
		if (!$name) {
			next;
		}
		
		# check if this exchange is just now being marked late
		# add it to the email body if it is
		if (!%prevhash || ($prevhash{$exchange}{state} eq 'wait' && $state eq 'late')) {
			$email_body .= "$name [$exchange]\n";
		}
		
		my $hashdump = join(',',($name, $exchange, $to, $dom, $dow, $vol, $to2, $vol2, $count, $state));
		#print "$hashdump\n";
		my $curdate = FeedForecast::currtime();
		# insert/update into database if not already complete
		my $nndb_insert = $nndb->prepare("
			begin tran
			declare \@state varchar(50)
			set \@state = (select [state] from DaemonLogs where ExchID = '$exchange' and [Date] = '$date')
			if (\@state is not NULL)
			begin
				if \@state != 'recv'
					update DaemonLogs set 
					CurrentVolume = '$count', 
					State = '$state',
					InsDateTime = '$curdate'
					where ExchID = '$exchange' and Date = '$date'
			end
			else
			begin
				insert into DaemonLogs (Date, ExchID, CurrentVolume, State, InsDateTime) values
									('$date','$exchange','$count','$state','$curdate')
			end
			commit tran");
		$nndb_insert->execute();
		print OLOG $hashdump . "\n";
	}
	close OLOG;
	
	$ds2->disconnect();
	$nndb->disconnect();
	
	# email notification with late feeds if there is a new late feed
	if ($email_body) {
		$email_body = "The following exchange(s) have been marked as late:\n$email_body";
		my $subject_line = '';
		FeedForecast::send_email($email_body,$subject_line,1,$config->smtp_server());
	}
	
	# create new Excel sheet
	system("perl generate_report.pl $date") == 0 or warn "could not create spreadsheet: $!\n";
}

# load the previous exchange log into a hash
sub load_log {
	my $date = shift;
	open (LOG, '<', sprintf($daemon_log,$date)) or return ();
	my %exchhash = ();
	while (<LOG>) {
		chomp;
		my @line = split ',';
						($exchhash{$line[1]}{name},
						$exchhash{$line[1]}{to},
						$exchhash{$line[1]}{dom},
						$exchhash{$line[1]}{dow},
						$exchhash{$line[1]}{vol},
						$exchhash{$line[1]}{to2},
						$exchhash{$line[1]}{vol2},
						$exchhash{$line[1]}{count},
						$exchhash{$line[1]}{state}) = @line[0,2..9];
	}
	close LOG;
	return %exchhash;
}

sub server_time_offset {
	my ($prev_flag) = @_;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);	
	return $hour * 60 + $min + 1440 + (1440 * $prev_flag); 
}