#!perl -w

# this is a configuration script for this feed
# subs will be exported as needed

package DS2PrimQtPrc;

use strict;
use File::stat;
use Time::localtime;
use Exporter;
use Scalar::Util qw(looks_like_number);
use lib '..';
use FeedForecast;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(init calc_metrics build_training run_nets updated);

# load config variables
my $config = FeedForecast::loadConfig();

my %codes = (
	offset_in => 199,
	dom_in => 200,
	dow_in => 201,
	vol_in => 202,
	vol_out => 203,
	offset_out => 204,
	exchid_id => 205,
	exchname_id => 206,
	date_id => 207,
	finishtime_his => 208,
	vol_his => 209,
);


1;

# initialization function
sub init {
	# check/build exchange log
	load_exchanges();
}


sub calc_metrics {
	my $exchange_log = $config->exchange_log();
	
	open EXCH, '<', $exchange_log;
	my @exchanges = <EXCH>;
	close EXCH;
	
	# create a ForkManager
	my $forkManager = new Parallel::ForkManager($config->cm_procs());
	
	for (@exchanges) {
		$forkManager->start and next;
		# split exchange query results
		my ($exchcode, $exchname) = split(',', $_);
		
		wout(1, "compiling data for $exchname [$exchcode]...");
		
		# insert the metrics for this exchange into the database
		insert( calc_finish($exchcode), $exchname, $exchcode );
		$forkManager->finish;
	}
	$forkManager->wait_all_children;
}

sub insert {
	my (%finishtimes, $exchname, $exchcode) = @_;
	
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	open (TFILE, '>', $config->exchmetrics_dir() . "$exchname-$exchcode.log");
	
	wout(2, "writing times to database and log...");
	# prepare the insert query
	my $ins_query = FeedForecast::get_ins_query('history',5);
	my $nndb_insert = $nndb->prepare($ins_query);
	foreach my $date (sort (keys %finishtimes)) {
		print TFILE "$date\t$finishtimes{$date}{'complete'}\t$finishtimes{$date}{'count'}\n";
		my @ins_values = (
			$codes{date_id} => $date,
			$codes{exchcode_id} => $exchcode,
			$codes{exchname_id} => $exchname,
			$codes{finishtime_his} => $finishtimes{$date}{complete},
			$codes{vol_his} => $finishtimes{$date}{count},
		);
		
		#print "inserting: $date,$exchcode,". asciiify($exchname).",$finishtimes{$date}{'complete'},$finishtimes{$date}{'count'}\n";
		
		$nndb_insert->execute(@ins_values);
	}
		
	close TFILE;
	
	$nndb->disconnect();
}

sub calc_finish {
	my ($exchcode) = @_;
	
	# date to begin collecting training data
	my $date_begin_init = $config->date_begin_init();
	
	# open connections to DBs
	my $disfl = DBI->connect($config->disfl_connection()) or die("Couldn't connect to DISFL: $!\n");
	my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
	my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	# get the date to start from from the nndb
	my $get_start = $nndb->prepare("select max(b.value) from history a
									inner join history b
									on a.row_id = b.row_id  
									where 
									a.code_id = $codes{exchid_id}
									and b.code_id = $codes{date_id}
									and a.value = ?");
	
	
	
	# query to load all transactions for infocodes within an exchange
	my $get_trans = $ds2_c->prepare("select [...], MarketDate, infocode 
		from [DataStream2_Change].[arc].[DS2PrimQtPrc] 
		where ExchIntCode = ? 
		and RefPrcTypCode = 1 
		and MarketDate > ?");
	
	
	# get execution time from marketinfo table
	my $get_marketinfo = $disfl->prepare("select ExecutionDateTime, BuildNumber,MakeUpdateSequence,MakeUpdateRunDate
		from [DISForLegacy].[dbo].[MakeUpdateInfo] with (NOLOCK)
		where DISTransactionNumber = ?
		and DataFeedId = 'DS2_EQIND_DAILY'");
	
	# get execution time from older Transactions table in DataIngestionInfrastructure
	#my $get_transaction = $dii->prepare("select ExecStart, FeedBuildNumber [DataIngestionInfrastructure].[dbo].[Transactions] with (NOLOCK)
	#	where FeedId = 'DS2_EQIND_DAILY'
	#	and Number = ?");
	wout(2, "getting date to start from...");
	$get_start->execute($exchcode);
	my @gstart = $get_start->fetchrow_array();
	$get_start->finish();
	my $date_begin = $date_begin_init;
	if ($gstart[0]) {	
		$gstart[0] =~ m/(\d{4})-(\d{2})-(\d{2})/;
		$date_begin = sprintf("%u%02u%02u", $1,$2,$3);
	}
	wout(2, "starting from $date_begin");
	wout(2,"retrieving transactions...");
	
	$get_trans->execute($exchcode, $date_begin);
	wlog(2, "done with query");
	
	wout(2,"examining transactions...");
	# hash for tracking transactions that for some reason do not exist in both tables
	my %errors = ();
	# hash for tracking the transaction count per date
	my %trans_info = ();
	# hash for tracking MarketInfo table results per transaction ID
	my %marketinfo = ();
	# hash for tracking infocode transactions
	my %infocodes = ();
	
	# look at each transaction 
	while (my @trans = $get_trans->fetchrow_array()) {
		my ($t_id, $marketdate, $infocode) = @trans;
		
		wlog(3, "fetched row: $t_id, $marketdate, $infocode");
		
		# skip this row if it is in the error hash
		if (exists $errors{$t_id}) {
			wlog(3, "row failed to match");
			next;	
		}
		# look at marketinfo row and get execution date + build num for ref
		if (! exists $marketinfo{$t_id}) {
			$get_marketinfo->execute($t_id);
			my @mi_row = $get_marketinfo->fetchrow_array();
			$get_marketinfo->finish();
			# insert code to find row on another table here (DS)
			if (! $mi_row[0]) {
				 wlog(3, "row failed to match: $t_id");
				 $errors{$t_id} = 1;
				 next;
			}
			# check if execution time is too far in the future, throw out
			#elsif (compedtmd($mi_row[0],$marketdate)) {
			#	wlog(3, "row failed to match");
			#	 $errors{$t_id} = 1;
			#	 next;
			#}
			
			# buildnum, filenum, filedate for future NN inputs
			wlog(4, "added new marketinfo row: $mi_row[0], $mi_row[1], $mi_row[2], $mi_row[3]");
			($marketinfo{$t_id}{'edt'}, $marketinfo{$t_id}{'bnum'}, $marketinfo{$t_id}{'fnum'}, $marketinfo{$t_id}{'fdate'}) = @mi_row;
		}
		
		$trans_info{$marketdate}{'count'}++;
		push @{$infocodes{$infocode}{$marketdate}}, $t_id;
		
		#push @{$trans_info{$marketdate}{'ids'}}, $t_id;
	}
	$get_trans->finish();

	wout(2, "examining infocode transactions...");

	# find the earliest 'complete' transaction for each infocode
	foreach my $ic (keys %infocodes) {
		foreach my $date (keys %{$infocodes{$ic}}) {
			my @edates = ();
			#wout (2, "working on $ic:$date\n");
			foreach my $t_id (@{$infocodes{$ic}{$date}}) {
				#wout(3, "pushing to array $marketinfo{$t_id}{'edt'}");
				push @edates, $marketinfo{$t_id}{'edt'};
			}
			@edates = sort @edates;
			#wout(2,"lowest exec date: $edates[0]");
			push @{$trans_info{$date}{'edts'}}, $edates[0];
		}
	} 

	# count number of infocodes for a particular date/exchange
	my $get_md_counts = $ds2->prepare("select marketdate, count(infocode) 
			from [DataStream2].[dbo].[DS2PrimQtPrc] with (NOLOCK)
			where marketdate > $date_begin
			and exchintcode = $exchcode
			group by marketdate
			order by 1 ASC");
			
	wout(2, "retrieving infocode counts...");
	$get_md_counts->execute();
	
	

	# hash for tracking the finish times per date
	my %finishtimes = ();
		
	wout(2, "calculating finish times...");
	# look at each date and calculate completion time
	while (my @md_counts = $get_md_counts->fetchrow_array()) {
		my ($date, $mdcount) = @md_counts;
		
		# skip for dates that don't exist for whatever reason
		if (! exists $trans_info{$date}) {
			wout(3, "skipping $date");
			next;
		}
		
		wlog(3,"examining date: $date");
		my @trans_times = ();
		my %buildnums = ();
		# get all execution times for that day in an array
		#foreach my $t_id (@{$trans_info{$date}{'ids'}}) {
		#	push @trans_times, $marketinfo{$t_id}{'edt'};
		#	$buildnums{$marketinfo{$t_id}{'bnum'}} = 1;
		#}
		# sort times ascending
		my @sorted = sort @{$trans_info{$date}{'edts'}};
		
		# calculate when we have received $completion% of the transactions
		# take that time as completion time for the date
		# figure out when 98% of mdcount is reached, not 98% of the total...
		@trans_times = splice(@sorted, 0, $config->calc_completion() * $mdcount);
		$finishtimes{$date}{'complete'} = $trans_times[-1];
		$finishtimes{$date}{'count'} = $mdcount;
		$finishtimes{$date}{'builds'} = scalar (keys %buildnums);
	}
	
	$get_md_counts->finish();
	
	$disfl->disconnect();
	$ds2_c->disconnect();
	$ds2->disconnect();
	$nndb->disconnect();
	
	return %finishtimes;

}



# retrieve exchanges from database if log not present
sub load_exchanges {
	
	my $exchange_log = $config->exchange_log();
	# date to begin collecting training data
	my $date_begin_init = $config->date_begin_init();
	
	if (! -e $exchange_log) {
		my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
		my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
		

		my $get_exchanges = $ds2_c->prepare("SELECT distinct ExchIntCode
  			FROM [DataStream2_Change].[arc].[DS2PrimQtPrc]
  			where MarketDate > '$date_begin_init'");
  			
  		my $get_exchname = $ds2->prepare("SELECT [ExchName]
  			FROM [DataStream2].[dbo].[DS2Exchange] with (NOLOCK) 
  			where ExchIntCode = ?");
		wout(1,"retrieving exchange list from server...");
		$get_exchanges->execute();
		open(EXCHLOG, '>', $exchange_log);
		while (my @row = $get_exchanges->fetchrow_array()) {
			if (! $row[0]) {
				next;
			}
			$get_exchname->execute($row[0]);
			my @exchname = $get_exchname->fetchrow_array(); 
			
			#asciiify(\$row[1]);
			print EXCHLOG "$row[0],$exchname[0]\n";
			
			# check directory exists for this exchange
			#if (! -d "logs/$row[0]") {
			#	mkdir("logs/$row[0]");
			#}
		}
		$get_exchanges->finish();
		close EXCHLOG;
		$ds2_c->disconnect();
		$ds2->disconnect();
	}
	else {
		my $creation = ctime(stat($exchange_log)->mtime);
		wout(1,"found exchange log ($creation)");
	}		
}

sub build_training {
	require List::Util;
	import List::Util qw(shuffle);
	

	# path to www directory for web server
	my $chartdir = $config->chartdir();
	# path to NN train + creation executable
	my $cascade_path = $config->cascade_path();
	my $nets_dir = $config->nets_dir();
	
	# set flag to generate test data sets
	my $test_flag = $config->test_flag();
	# shuffle training data before generating data sets
	my $shuffle = $config->shuffle_flag();
	# percent of total data to use as test data
	my $test_perc = $config->test_perc();
	
	
	if (! -d $nets_dir) {
		print "no $nets_dir dir found, creating one...\n";
		mkdir($nets_dir) or die("could not create log dir: $!\n");
	}
	
	my $logdir = $config->bt_logdir();
	
	if (! -d $logdir) {
		print "no $logdir dir found, creating one...\n";
		mkdir($logdir) or die("could not create log dir: $!\n");
	}
	
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");  
	
	my $nndb_exchanges = $nndb->prepare("select distinct a.value
										from history a
										inner join history b 
										on a.row_id = b.row_id
										where
										a.code_id = $codes{exchid_id}");
	
	$nndb_exchanges->execute();
	my $exchanges = $nndb_exchanges->fetchall_arrayref();
	$nndb_exchanges->finish();
	$nndb->disconnect();
	# create a ForkManager to manage forking training processes
	my $forkManager = new Parallel::ForkManager($config->training_procs());
	
	foreach my $exchange (@{$exchanges}) {
		# fork a new process if needed
		$forkManager->start and next;
		
		my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");  
		
		my $nndb_count = $nndb->prepare("select count(*) from history
						where
						code_id = $codes{exchid_id}");
		
		my $nndb_all = $nndb->prepare("select a.value, b.value, c.value 
			from history a 
			inner join history b on a.row_id = b.row_id
			inner join history c on b.row_id = c.row_id
			inner join history d on c.row_id = d.row_id
			where
			a.code_id = $codes{date_id}
			and b.code_id = $codes{finishtime_his}
			and c.code_id = $codes{vol_his}
			and d.code_id = $codes{exchid_id}
			and d.value = ?
			order by a.value asc");
		
		my ($exchid, $exchname) = @{$exchange};
		open ELOG, '>', "$logdir/$exchname-$exchid.log";
		print FeedForecast::currtime() . "\twriting training and test data for $exchname [$exchid]\n";
		print ELOG "counting number of history records...";
		$nndb_count->execute($exchid);
		my @datacount = $nndb_count->fetchrow_array();
		my $datacount = $datacount[0];
		$nndb_count->finish();
		print ELOG "$datacount\n";
		
		open (TRAIN, '>',"$logdir/$exchname-$exchid.xtrain");		
		open (TRAIN1, '>',"$logdir/$exchname-$exchid-1.xtrain");
		open (TRAIN3, '>',"$logdir/$exchname-$exchid-3.xtrain");
		
		# write training file header
		print TRAIN "$datacount 4 2\n";
		print TRAIN1 "$datacount 4 1\n";
		print TRAIN3 "$datacount 4 1\n";
		
		# configure Excel chart
		my $workbook = Spreadsheet::WriteExcel->new("$chartdir/$exchname-$exchid.xls");
		my $worksheet = $workbook->add_worksheet();
		my $timechart = $workbook->add_chart( type => 'line', name => 'time chart');
		$timechart->add_series(
			values => "=Sheet1!\$A\$1:\$A$datacount",
		);
		my $volchart = $workbook->add_chart( type => 'line', name => 'volume chart');
		$volchart->add_series(
			values => "=Sheet1!\$B\$1:\$B$datacount",
		);
		my $timedata = [];
		my $voldata = [];
		
		my $first = 1;
		my $last = 0;
		my @points = ();
		my ($ptimeoffset, $pmday, $pwday, $pvolume);
		#print ELOG "retrieving all training data...\n";
		$nndb_all->execute($exchid);
		my $rowcount = 0;
		while (my @row = $nndb_all->fetchrow_array()) {
			# check if this is the last element in the array
			if (++$rowcount == $datacount) {
				$last = 1;
			}	
			
			my ($date, $finish, $volume) = @row;
			if (!$finish || !$volume) {
				#print LOG "missing record column, skipping $date\n";
				next;
			}
			
			# calculate dow, dom and next day flag, time of day offset
			my ($mday, $wday, $timeoffset) = parsedate($date, $finish);
			
			# if not generating a test data set, just write to file
			# also, much less memory intensive for large data sets	
			if (!$test_flag) {
				# print result set for previous input row
				if (! $first) {
					print TRAIN "$timeoffset $volume\n";
					print TRAIN1 "$timeoffset\n";
					print TRAIN3 "$volume\n";
					push @{$timedata}, $timeoffset;
					push @{$voldata}, $volume;
				}
				else {
					$first = 0;
				}
				
				# print current input row
				if (! $last) {
					print TRAIN "$timeoffset $mday $wday $volume\n";
					print TRAIN1 "$timeoffset $mday $wday $volume\n";
					print TRAIN3 "$timeoffset $mday $wday $volume\n";
				}	
			}
			# if generating a test data set, create tuples of data points
			else {
				if (!$first) {
					push @points, [ 
							[$ptimeoffset, $pmday, $pwday, $pvolume],
							[$timeoffset, $volume]];
					push @{$timedata}, $timeoffset;
					push @{$voldata}, $volume;
					#print "added point: [$ptimeoffset, $pmday, $pwday, $pvolume],
					#		[$timeoffset, $volume]\n";
				}
				else {
					$first = 0;
				}
				
			}
			($ptimeoffset, $pmday, $pwday, $pvolume) = ($timeoffset, $mday, $wday, $volume);
		}
		$nndb_all->finish();
		
		# handle data points if flag enabled...
		if ($test_flag) {
			# if there were no points, skip this exchange...
			if (!scalar(@points)) {
				next;
			}
			# shuffle points
			if ($shuffle) {
				@points = shuffle(@points);
			}
			# split into training and test data
			my @test = @points[0 .. ($test_perc * scalar(@points))];
			my @train = @points[($test_perc * scalar(@points) + 1) .. $#points];
			open (TN, '>',"$logdir/$exchname-$exchid.train");
			print TN scalar(@train) . " 4 2\n";
			open (TT, '>',"$logdir/$exchname-$exchid.test");
			print TT scalar(@test) . " 4 2\n";
			#print $test[0][0][0];
			foreach (@test) {
				my ($timeoffset, $mday, $wday, $volume, $ntimeoffset, $nvolume) = (@{$_->[0]}, @{$_->[1]});   
				print TT "$timeoffset $mday $wday $volume\n$ntimeoffset $nvolume\n";
			}
			foreach (@train) {
				my ($timeoffset, $mday, $wday, $volume, $ntimeoffset, $nvolume) = (@{$_->[0]}, @{$_->[1]});   
				print TN "$timeoffset $mday $wday $volume\n$ntimeoffset $nvolume\n"; 
			}
			close TT;
			close TN;
		}
			
		close TRAIN;
		close TRAIN1;
		close TRAIN3;
		
		# generate Excel spreadsheet
		$worksheet->write('A1', [$timedata]);
		$worksheet->write('B1', [$voldata]);
		
		# train specified number of iterations of networks
		# pick the best one to use
		my %best_net;
		foreach my $iteration (1..$config->training_iterations()) {
			# generate net file using FANN binary
			my $command = "\"$cascade_path\" \"$logdir\\$exchname-$exchid.test\" \"$logdir\\$exchname-$exchid.train\" \"$nets_dir\\tmp\\$exchname-$exchid.net.$iteration\"";
			my $result = `$command`;
			# regex parse this for the interesting bits...
			$result =~ m/Train outputs    Current error: (.*). Epochs   (\d*)\n/;
			my ($train_error, $epochs) = ($1, $2); 
			$result =~ m/Train bit-fail: (\d*),/;
			print ELOG "$iteration\tError: $train_error\n\tBit Fail: $1\n\tEpochs: $epochs\n\n";
			# update best network
			if ($iteration == 1 || ($train_error < $best_net{error})) {
				$best_net{error} = $train_error;
				$best_net{iteration} = $iteration;
			}
			# throw out exchanges that give crazy results
			# they seem to take forever to calculate and result doesn't change
			last if !looks_like_number($train_error);  
		}
		
		print ELOG "net selected: " . $best_net{iteration} . " with error of: " . $best_net{error} . "\n\n";
		# move the best network to the nets directory
		copy("$nets_dir\\tmp\\$exchname-$exchid.net." . $best_net{iteration}, "$nets_dir\\$exchname-$exchid.net");
		
		close ELOG;
		
		# exit child process
		$forkManager->finish;
	}
	
	$forkManager->wait_all_children;
		
}

sub run_nets {
	
	my $netexe = $config->net_exe();
	my $exchlog = $config->exchange_log();
	my $networkdir = $config->nets_dir();
	my $dryrun = $config->runnet_dryrun();
	
	# automatically skip weekends when calculating previous day
	my $weekend_flag = $config->weekend_flag();
	
	my $runnet_log = sprintf($config->runnet_log(),FeedForecast::calc_date());
	
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
			
			my $ins_query = FeedForecast::get_ins_query('results',9);
			
			
			my $nndb_insert = $nndb->prepare($ins_query);
			
			my @insert_values = (
				$codes{date_id} => "convert(varchar, convert(datetime, '$date'), 121)",
				$codes{exchid_id} => $exchid,
				$codes{exchname_id} => $exchname,
				$codes{offset_in} => $timeoffset,
				$codes{dom_in} => $dom,
				$codes{dow_in} => $dow,
				$codes{vol_in} => $vol,
				$codes{offset_out} => $to2,
				$codes{vol_out} => $vol2,
			);
			print $ins_query;
			$nndb_insert->execute(@insert_values) if !$dryrun;
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

# calculate yesterday's date and format for queries
sub y_date {
	# automatically skip weekends when calculating previous day
	my $weekend_flag = $config->weekend_flag();
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

sub updated {

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
			print FeedForecast::currtime() . "\tfinished previous day update task\n";
			exit;
		}
		
		print FeedForecast::currtime() . "\tfinished current day update task\n";
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
		
		my $select_result = $nndb->prepare("select 
		   d.value exchid, e.value exchname, f.value input_offset, 
		   b.value dom, c.value dow, g.value input_vol, h.value output_offset, i.value output_vol 
				from results a 
				inner join results b on a.row_id = b.row_id
				inner join results c on b.row_id = c.row_id
				inner join results d on c.row_id = d.row_id
				inner join results e on d.row_id = e.row_id
				inner join results f on e.row_id = f.row_id
				inner join results g on f.row_id = g.row_id
				inner join results h on g.row_id = h.row_id
				inner join results i on h.row_id = i.row_id
				where
				a.code_id = $codes{date_id}
				and a.value = convert(varchar, convert(datetime, '?'), 121)
				and b.code_id = $codes{dom_in}
				and c.code_id = $codes{dow_in}
				and d.code_id = $codes{exchid_id}
				and e.code_id = $codes{exchname_id}
				and f.code_id = $codes{offset_in}
				and g.code_id = $codes{vol_in}
				and h.code_id = $codes{offset_out}
				and i.code_id = $codes{vol_out}
				order by a.value asc
			");
		
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
			
			#if ($prevhash{$exchange}{state} ne 'recv' && $state eq 'recv') {
			#	print "$name $date state: ". $prevhash{$exchange}{state} . "\n";
			#}
			
			
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
				set \@state = (select [state] from DS2PrimQtPrc where ExchID = '$exchange' and [Date] = '$date')
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
		
		# try to create an independant process to handle filenum,fildate,buildnum updates
		system 1, "perl update_completed.pl $date";
		
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
}






sub server_time_offset {
	my ($prev_flag) = @_;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);	
	return $hour * 60 + $min + 1440 + (1440 * $prev_flag); 
}
