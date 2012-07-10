#!/usr/bin/perl -w

# build all training data from historical data

my $start_time = time;

use strict;
use DBI;
use Date::Calc qw(:all);
use List::Util qw(shuffle);
use Spreadsheet::WriteExcel;
use File::Copy;
use Scalar::Util qw(looks_like_number);
use Parallel::ForkManager;
use Time::Duration;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $logdir = $config->bt_logdir();
my $btlog = $config->bt_log();
open(LOG, '>', $btlog);
print LOG FeedForecast::currtime() . "\tbuildtraining routine started...\n";

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

if (! -d $logdir) {
	print "no $logdir dir found, creating one...\n";
	mkdir($logdir) or die("could not create log dir: $!\n");
}
if (! -d $nets_dir) {
	print "no $nets_dir dir found, creating one...\n";
	mkdir($nets_dir) or die("could not create log dir: $!\n");
}

my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");  
	
my $nndb_exchanges = $nndb->prepare("select distinct ExchID, ExchName from FinishTimes");

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
			@points = shuffle @points;
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


print LOG FeedForecast::currtime() . "\tdone in " . duration(time - $start_time) . "\n";
close LOG;

# convert a time offset to an excel datetime string
sub offset_datetime {
	
}

# calculate weekday from julian
sub parsedate {
	my ($date, $finish) = @_;
	$date =~ m/(\d{4})-(\d{2})-(\d{2})/;
	my ($y,$m,$d) = ($1,$2,$3);
	$date = FeedForecast::julianify($date);
	
	
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
	my $timeoffset = $1 * 60 + $2 + $initoffset;
	
	return (int($d), Day_of_Week($y, $m, $d),$timeoffset);
}