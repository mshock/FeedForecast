#!perl
# called by update_completed, updates the score for a day and the total score

use strict;
use DBI;
use Date::Manip qw(ParseDate Date_Cmp DateCalc UnixDate);
use FeedForecast;

my $marketdate = $ARGV[0];
my $exchid = $ARGV[1];

my $config = FeedForecast::loadConfig();

my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");

# get count of all recv & updated for scaling
#my $count_query = $nndb->prepare("
#		select count(ExchID) from DaemonLogs where 
#			State = 'recv' and 
#			Date = '$marketdate' and 
#			BuildNumber is not null and
#			FileNumber is not null and 
#			FileDate is not null");
#$count_query->execute();
#my $count_recv = ($count_query->fetchrow_array())[0];
#$count_query->finish();

my $select_query = $nndb->prepare("
		select InputVolume, CurrentVolume, OutputVolume, dl.InsDateTime, InputOffset, OutputOffset
				from Daemonlogs dl 
				inner join 
					NetResults nr on
					dl.ExchID = nr.ExchID and
					dl.Date = nr.Date 
				where
				dl.ExchID = ? and
				dl.Date = '$marketdate'");
				
$select_query->execute($exchid);
my @select_result = $select_query->fetchrow_array();
$select_query->finish();

my ($prevvol, $curvol, $predvol, $recvtime, $prevtime, $predtime)  = @select_result;



# get time score
my $time_score = compare_times($prevtime, $predtime, $recvtime); 

# get volume score
my $vol_score = compare_volumes($prevvol, $curvol, $predvol);

# adjust scores base on number of recvd
#($recv_count-1) *  
#$time_score = $time_score 

# insert or update DailyScore for the date

# check that the query was successful
if (!$recvtime) {
	print "\t[$marketdate] $exchid: no query result, zeroing score\n";
	$time_score = 0;
	$vol_score = 0;
}

my $update_score = $nndb->prepare("update DaemonLogs set
			VolumeScore = ?,
			TimeScore = ?
			where
			Date = '$marketdate' and
			ExchID = ?");


$update_score->execute($vol_score, $time_score, $exchid);

print "[$marketdate] <$exchid> vscore: $vol_score\ttscore: $time_score\n";

$update_score->finish();
$nndb->disconnect();

# compare volumes
sub compare_volumes {
	my ($previous, $actual, $predicted) = @_;
	
	# 2 points for exactly correct
	if ($actual == $predicted) {
		return 2;
	}
	# 1 point for getting the right direction
	elsif ((($predicted < $previous) && ($actual < $previous)) || 
	(($predicted > $previous) && ($actual > $previous))) {
		return 1;
	}
	# otherwise the prediction was just entirely wrong, -1 point
	return -1;
}

# compare times
sub compare_times {
	my ($previous_o, $predicted_o, $recvtime) = @_;
		
	# get offsets in DateTime format
	my $hr_previous = FeedForecast::calcTime($previous_o);
	my $hr_predicted = FeedForecast::calcTime($predicted_o);
	#print "previous: $hr_previous\npredicted: $hr_predicted\n";
	
	my $previous_dt = convertOffset($hr_previous, $marketdate);
	my $predicted_dt = convertOffset($hr_predicted, $marketdate);
	
	#print $predicted_dt;
	
	
	# parse with Date::Manip
	my $parsed_predicted = ParseDate($predicted_dt);
	my $parsed_previous = ParseDate($previous_dt);
	my $parsed_recvd = ParseDate($recvtime);
	#print "prev: " . UnixDate($parsed_previous, "%Y%m%d %T\n");
	#print "recv: " . UnixDate($parsed_recvd, "%Y%m%d %T\n");
	#print "pred: " . UnixDate($parsed_predicted, "%Y%m%d %T\n");
	# calculate metrics
	my $recvd_upperbound = DateCalc($parsed_recvd, 'in ' . $config->show_late() . ' minutes');
	my $recvd_lowerbound = DateCalc($parsed_recvd, $config->show_late() . ' minutes ago');
	
	
	
	# -1 for within
	my $within_upper = Date_Cmp($parsed_predicted, $recvd_upperbound);
	#print "upper: $within_upper\n";
	# 1 for within
	my $within_lower = Date_Cmp($parsed_predicted, $recvd_lowerbound);
	#print "lower: $within_lower\n";
	
	# +1 for later, -1 for earlier
	my $prediction_dir = Date_Cmp($parsed_predicted, $parsed_previous);
	my $recvd_dir = Date_Cmp($parsed_recvd, $parsed_previous);
	
	# check if within bounds
	my $within_bounds = $within_upper == -1 && $within_lower == 1 ? 1 : 0;
	
	# check if prediction was in the correct direction
	my $correct_dir = $recvd_dir == $prediction_dir ? 1 : 0;
	
	# return score
	return 2 if ($within_bounds && $correct_dir);
	return 1 if ($within_bounds);
	return -1 if ($correct_dir);
	return -2;
}

# convert an offset into human readable
sub convertOffset {
	my ($otime, $date) = @_;
	
	if ($otime =~ m/prev/) {
		($date,,) = FeedForecast::decrement_day($date);	
	}
	elsif ($otime =~ m/next/) {
		$date =~ m/(\d{4})(\d{2})(\d{2})/;
		my $tmpdate = "$1-$2-$3";
		$date = FeedForecast::increment_day($tmpdate);
	}
	$date =~ m/(\d{4})(\d{2})(\d{2})/;
	$date = "$1-$2-$3";
	
	$otime =~ m/(\d+:\d+)/;
	return "$date $1";
}