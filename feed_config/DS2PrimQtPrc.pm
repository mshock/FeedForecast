#!perl 

# this is a configuration script for this feed
# will be imported, subs will be called

package DS2PrimQtPrc;

use strict;
use Exporter;
use lib '..';
use FeedForecast;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(init calc_metrics build_training run_nets);

# load config variables
my $config = FeedForecast::loadConfig();

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
			207 => $date,
			205 => $exchcode,
			206 => $exchname,
			208 => $finishtimes{$date}{complete},
			209 => $finishtimes{$date}{count},
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
									a.code_id = 205
									and b.code_id = 207
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
		
		# get a list of all the exchanges present in DS2 
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
										a.code_id = 205");
	
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
		
		my $nndb_count = $nndb->prepare("select count(*) from FinishTimes where ExchID = ?");
		my $nndb_all = $nndb->prepare("select * from FinishTimes where ExchID = ? order by Date ASC");
		
		
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
			
			my ($date, $finish, $volume) = ($row[1],$row[4],$row[5]);
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
