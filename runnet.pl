#!/usr/bin/perl -w

# daemon for NN validation
# runs all NNs to generate forecasting data
# should be added to windows task scheduler (AT) at daily intervals

use strict;
use DBI;
use Date::Calc qw(Add_Delta_Days Day_of_Week);
use Parallel::ForkManager;
use Scalar::Util qw(looks_like_number);
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $netexe = $config->net_exe();
my $exchlog = $config->exchange_log();
my $networkdir = $config->nets_dir();
my $dryrun = $config->runnet_dryrun();

# automatically skip weekends when calculating previous day
my $weekend_flag = $config->weekend_flag();

my $runnet_log = sprintf($config->runnet_log(),FeedForecast::calc_date());

print FeedForecast::currtime() . "\trunning nets\n\n";

run_nets();

print FeedForecast::currtime() . "\tdone.\n";

print "updating score report...\n";
`perl score_report.pl`;
print "done.\n";

# subs

sub run_nets {
	
	# get all networks
	opendir(my $dh, $networkdir);
	my @networks = readdir($dh);
	closedir($dh);
	
	# clear log file
	open LOG, '>', $runnet_log;
	print LOG '';
	close LOG;
	
	# run each network
	
	my $forkManager = new Parallel::ForkManager($config->runnet_procs());
	foreach my $network (@networks) {
		$forkManager->start and next;
		my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
		if ($network =~ m/^(.*)-(\d*)\.net/) {
			my $exchname = $1;
			my $exchid = $2;
			my $exectime = time;
			#print "file: $network\nexchange: $exchname\nid: $exchid\n";
			# get most recent day's metrics from database
			my ($timeoffset, $dom, $dow, $vol) = divine_metrics($exchname, $exchid);
			my $date = FeedForecast::calc_date();
			# catch errors, skip this exchange
			if ($timeoffset eq "error") {
				#print "skipping $exchname:$exchid\n\n";
				# got to stop child thread, otherwise it will enter another loop
				$forkManager->finish;
				next;
			}
			#print "metrics divined: $timeoffset, $dom, $dow, $vol\n";
			# execute network over these metrics	
			$netexe =~ s/\//\\/g;
			#print "\"$netexe\" test $timeoffset $dom $dow $vol";
			my $result = `\"$netexe\" \"$networkdir\\$network\" $timeoffset $dom $dow $vol`;
			$exectime = time - $exectime;
			#print "execution time: $exectime sec\n\n";
			# round the results
			my @result = split(',', $result);
			if (!looks_like_number($result[0]) || !looks_like_number($result[1])) {
				print FeedForecast::currtime() . "\t$exchname:\t$timeoffset, $dom, $dow, $vol = bad network output\n";
				$forkManager->finish;
				next;
			}
			print FeedForecast::currtime() . "\t$exchname:\t$timeoffset, $dom, $dow, $vol = $result\n";	
			
			my $to2 = int($result[0] + .5);
			my $vol2 = int($result[1] + .5);
			my $curdate = FeedForecast::currtime();
			my $nndb_insert = $nndb->prepare("insert into NetResults 
				(Date, ExchID, ExchName, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, InsDateTime) 
				values
				('$date','$exchid','$exchname','$timeoffset','$dom','$dow','$vol','$to2','$vol2','$curdate')");
			$nndb_insert->execute() if !$dryrun;
			$nndb_insert->finish();
			open LOG, '>>', $runnet_log;
			print LOG "$exchname,$exchid,$timeoffset,$dom,$dow,$vol,$to2,$vol2\n";
			close LOG;
			$nndb->disconnect();
			
		}
		$forkManager->finish;
	}
	$forkManager->wait_all_children;
}

sub divine_metrics {
	my ($exchname, $exchid) = @_;
	#open LOG, '>', "log.txt";
	
	  
	my $disfl = DBI->connect($config->disfl_connection()) or die("Couldn't connect to DISFL: $!\n");
	my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
	my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
	
	
	my ($date, $mday, $wday) = y_date();
	#print "calculated date metrics: $date, $mday, $wday\n";
	
	my $get_md_counts = $ds2->prepare("select count(infocode) 
			from [DataStream2].[dbo].[DS2PrimQtPrc] with (NOLOCK)
			where marketdate = ?
			and exchintcode = $exchid
			group by marketdate
			order by 1 ASC");
			
	my $found_flag = 1;
	my $decrement_counter = 0;
	my @md_count = ();
	while ($found_flag) {
		#print "executing query to find transaction volume on $date...";
		# calculate completion time for this date
		$get_md_counts->execute($date);
		#print "done\n";
		@md_count = $get_md_counts->fetchrow_array();
		
		if (!$md_count[0]) {
			if (++$decrement_counter == 7) {
				#print "no records found for $exchname:$exchid for the past week\n";
				return ("error",0,0,0);
			}
			
			#print "no records for volume at $date, rolling back a day\n";
			($date, $mday, $wday) = FeedForecast::decrement_day($date);
		}
		else {
			$found_flag = 0;
		}
	}
	$get_md_counts->finish();
	
	# query to load all transactions for an infocode within an exchange
	my $get_trans = $ds2_c->prepare("select [...], MarketDate, infocode 
		from [DataStream2_Change].[dbo].[DS2PrimQtPrc] with (NOLOCK)
		where ExchIntCode = ? 
		and RefPrcTypCode = 1 
		and MarketDate = '$date'");
	
	# get execution time from marketinfo table
	my $get_marketinfo = $disfl->prepare("select ExecutionDateTime, BuildNumber
		from [DISForLegacy].[dbo].[MakeUpdateInfo] with (NOLOCK)
		where DISTransactionNumber = ?
		and DataFeedId = 'DS2_EQIND_DAILY'");
  
  	#print "running query to find all transactions for exchange...";
  	$get_trans->execute($exchid);
  	#print "done\n";
	my %transactions = ();
	my %errors = ();
	my %infocodes = ();
	
	#print "compiling all transactions...";
	while (my @t = $get_trans->fetchrow_array()) {
		my ($tid, $mdate, $infocode) = @t;
		#print "$tid $mdate $infocode\n";
		# keep has of failed transactions to skip
		if (exists $errors{$tid}) {
			next;
		}
		# otherwise retrieve transaction into hash
		if (!exists $transactions{$tid}) {
			$get_marketinfo->execute($tid);
			my @mi_row = $get_marketinfo->fetchrow_array();
			$get_marketinfo->finish();
			# insert code to find row on another table here (DS)
			if (! $mi_row[0]) {
				 $errors{$tid} = 1;
				 next;
			}
			#print LOG "adding new transaction: $mi_row[0]\n";
			$transactions{$tid} = $mi_row[0];
		}
		#print LOG "pushing to $infocode : $transactions{$tid}\n";
		push @{$infocodes{$infocode}}, $transactions{$tid};
	}
	$get_trans->finish();
	#print "done\n";
	#close LOG;

	
	#print "getting earliest execution time per infocode...";
	my @trans_info = ();
	# get earliest complete transaction for each infocode
	foreach my $ic (keys %infocodes) {
		
		my @edates = sort @{$infocodes{$ic}};
		#wout(2,"lowest exec date: $edates[0]");
		push @trans_info, $edates[0];	
	}
	
	
	@trans_info = sort @trans_info;
	@trans_info = splice(@trans_info, 0, .98 * $md_count[0]);
	# quick check for db inconsistancy
	if (!scalar(@trans_info)) {
		#print "\nbad number of change records (wtf)...\n";
		return ("error",0,0,0);
	}
	my $offset = calc_offset($date,$trans_info[-1]);
	#print "done\n";
	
	$disfl->disconnect();
	$ds2->disconnect();
	$ds2_c->disconnect();
	
	return ($offset, $mday, $wday, $md_count[0]);
}

sub calc_offset {
	my ($date, $finish) = @_;
	# calculate offset over 3 day period
	# day 2 being current day gmt
	# 1 previous days, 3 following days
	my $jfinish = FeedForecast::julianify($finish);
	my $initoffset = 1440;
	if ($jfinish > $date) {
		$initoffset = 2880;
	}
	elsif ($jfinish < $date) {
		$initoffset = 0;
	}
	
	$finish =~ m/(\d{2}):(\d{2}):\d{2}/;
	return $1 * 60 + $2 + $initoffset;
	
}

# read the exchange log from the training file data script
sub load_exchanges {
	open (EXCH, $exchlog);
	my @exchanges = <EXCH>;
	close EXCH;
	my %exchash = ();
	for (@exchanges) {
		chomp;
		my ($code, $name) = split ',';
		$exchash{$name} = $code;
	}
	return %exchash;
}

# calculate yesterday's date and format for queries
sub y_date {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	my $yesterday = time;
	do {
		$yesterday -= 86400;
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($yesterday);
		#print $wday . "\n";
	} while (($wday == 0 || $wday == 6) && $weekend_flag);
	
	my $date = sprintf("%u%02u%02u", $year + 1900,$mon + 1,$mday);
	return ($date, $mday, $wday);
}