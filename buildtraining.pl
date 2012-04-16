#!/usr/bin/perl -w
use strict;
use DBI;
use Date::Calc qw(:all);
use List::Util qw(shuffle);
use Spreadsheet::WriteExcel;
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

my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");  



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

my $nndb_all = $nndb->prepare("select * from FinishTimes where ExchID = ? order by Date ASC");
my $nndb_exchanges = $nndb->prepare("select distinct ExchID, ExchName from FinishTimes");
my $nndb_count = $nndb->prepare("select count(*) from FinishTimes where ExchID = ?");

$nndb_exchanges->execute();
my $exchanges = $nndb_exchanges->fetchall_arrayref();
$nndb_exchanges->finish();
foreach my $exchange (@{$exchanges}) {
	my ($exchid, $exchname) = @{$exchange};
	print LOG "writing training and test data for $exchname [$exchid]\n";
	print LOG "counting number of history records...";
	$nndb_count->execute($exchid);
	my @datacount = $nndb_count->fetchrow_array();
	my $datacount = $datacount[0];
	print LOG "$datacount\n";
	
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
	my $chart = $workbook->add_chart( type => 'line');
	$chart->add_series(
		values => "=Sheet1!\$A\$1:\$A$datacount",
	);
	my $chartdata = [];
	
	
	my $first = 1;
	my $last = 0;
	my @points = ();
	my ($ptimeoffset, $pmday, $pwday, $pvolume);
	print LOG "retrieving all training data...\n";
	$nndb_all->execute($exchid);
	my $rowcount = 0;
	while (my @row = $nndb_all->fetchrow_array()) {
		# check if this is the last element in the array
		if (++$rowcount == $datacount) {
			$last = 1;
		}
		
		my ($date, $finish, $volume) = ($row[1],$row[4],$row[5]);
		if (!$finish || !$volume) {
			print LOG "missing record column, skipping $date\n";
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
				push @{$chartdata}, $timeoffset;
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
				push @{$chartdata}, $timeoffset;
				#print "added point: [$ptimeoffset, $pmday, $pwday, $pvolume],
				#		[$timeoffset, $volume]\n";
			}
			else {
				$first = 0;
			}
			
		}
		($ptimeoffset, $pmday, $pwday, $pvolume) = ($timeoffset, $mday, $wday, $volume);
	}
	
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
	$worksheet->write('A1', [$chartdata]);
	
	# generate net file using FANN binary
	my $command = "\"$cascade_path\" \"$logdir\\$exchname-$exchid.test\" \"$logdir\\$exchname-$exchid.train\" \"$nets_dir\\$exchname-$exchid.net\"";
	my $result = `$command`;
	# regex parse this for the interesting bits...
	$result =~ m/Train outputs    Current error: (.*). Epochs   (\d*)\n/;
	my ($train_error, $epochs) = ($1, $2); 
	$result =~ m/Train bit-fail: (\d*),/;
	print LOG "\tError: $train_error\n\tBit Fail: $1\n\tEpochs: $epochs\n\n";
	


	
}


print LOG FeedForecast::currtime() . "\tdone\n";
close LOG;


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