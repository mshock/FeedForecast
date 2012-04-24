#/usr/bin/perl

# script to display webapp html

use strict;
use DBI;
use Getopt::Std;
use URI::Escape;
use Date::Manip qw(ParseDate Date_Cmp DateCalc);
use FeedForecast;

use vars qw($opt_d $opt_l $opt_s $opt_t);

getopts('d:ls:t:');

my $config = FeedForecast::loadConfig();

my $cur_page = $0;

#$connection = odbc_connect("Driver={SQL Server Native Client 10.0};Server=$server;Database=$DB;", $user, $pass) or die("failed to connect!");
my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n"); 

# get the date to look at
my $dbdate = FeedForecast::calc_date();
if ($opt_d && $opt_d =~ m/^\d{8}$/) {
	$dbdate = $opt_d;
}

# display only recv'd and late
my $late_checked = '';
if ($opt_l) {
	$late_checked = 'checked=true';
}

# search params
my $search = '';
my $exch_selected = '';
my $country_selected = '';
if ($opt_s && $opt_t) {
	if ($opt_t eq 'exchange') {
		$search = "and nr.ExchName = '$opt_s'";
		$exch_selected = 'selected=true'; 
	}
	elsif ($opt_t eq 'country') {
		$search = '';
		$country_selected = 'selected=true';
	}
}

my $printdate = pretty_date($dbdate);
if ($dbdate == FeedForecast::calc_date()) {
	$printdate = FeedForecast::currtime();
}


my $result = $nndb->prepare("select ExchName, nr.ExchID, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, CurrentVolume, State, dl.InsDateTime
				from NetResults nr join DaemonLogs dl 
				on nr.Date = dl.Date and nr.ExchID = dl.ExchID
				where 
				 nr.Date = '$dbdate'
				 $search");
$result->execute();

$dbdate =~ m/(\d{4})(\d\d)(\d\d)/;
my $pretty_date = sprintf("%u/%u/%u", $2, $3, $1);
my $nextdate = FeedForecast::increment_day("$1-$2-$3");
my ($prevdate, $trash1, $trash2) = FeedForecast::decrement_day($dbdate);

print "<html>
<head>
<meta http-equiv='refresh' content='300' > 
<link rel='stylesheet' type='text/css' href='styles.css' />
</head>
<body>
	
	<table cellspacing='0' id='fixedheader'>
		<thead>
		<tr>
			<th colspan='11' ><h2>Forecasts for $printdate</h2></th>
		</tr>
		<tr>
			<th colspan='2'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
			<th colspan='6'>
				<form method='GET'>
				<input type='submit' value='search' /> 
				<input type='button' value='reset' onclick='parent.location=\"?\"'/>
				<input type='text' name='date' value='$pretty_date' />
				<input type='text' name='search' value='$opt_s'/>
				<select name='search_type'>
					<option value='exchange' $exch_selected >Exchange</option>
					<option value='country' $country_selected >Country</option>
				</select>
				|
				<input type='checkbox' name='show_late' onclick='this.form.submit();' value='true' $late_checked/> Show Late
				<input type='hidden' name='date' value='$dbdate' />
				</form>
			</th>
			<th colspan='3'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
		</tr>
		<tr >
			<th>Exchange Name</th>
			<th>Exchange ID</th>
			<th>Previous Time</th>
			<th>Input DOM</th>
			<th>Input DOW</th>
			<th>Input Volume</th>
			<th>Forecasted Time</th>
			<th>Output Volume</th>
			<th>Recv'd Volume</th>
			<th>Recv DateTime</th>
			<th>Graph</th>
		</tr>
		</thead>
		<tbody>";

# loop over exchange array and print to table

my (@error,@late,@wait,@recv);
while(my @row = $result->fetchrow_array()) {

	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt) = @row;

	if ($state eq "recv") {
		push(@recv,[@row]);
	}
	elsif ($state eq "late") {
		push(@late,[@row]);
	}
	elsif ($state eq "wait") {
		push(@wait,[@row]);
	}
	else {
		push(@error,[@row]);
	}
}

# only get the recv'd rows if we're showing recv'd but late (checkbox)
my @rows = $opt_l ? (@late, @recv) : (@error, @late, @wait, @recv);
my $even_odd = 0;
my $eo = '';
foreach my $row (@rows) {
	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt) = @{$row};
	
	my $otime = calcTime($ooffset);
	
	# if showing late compare times to find late 
	if ($opt_l && $state eq 'recv') {
		next if (compareTimes($otime, $insdt) != -1);
	}
	
	if ($even_odd++ % 2) {
		$eo = 'odd';
	}
	else {
		$eo = 'even';
	}
	
	my $itime = calcTime($ioffset);
	
	$insdt =~ s/:\d\d\..*//;
	$insdt =~ s/0(\d:)/$1/;
	if (!$count) {
		$count = '---';
	}
	if (!($state eq 'recv')) {
		$insdt = '---';
	}
	# set background accordingly
	my $row_class = $state . '_' . $eo;
	
	print "<tr class='$row_class'>
	<td>$name</td>
	<td>$id</td>
	<td>$itime ($ioffset)</td>
	<td>$dom</td>
	<td>$dow</td>
	<td>$ivol</td>
	<td>$otime ($ooffset)</td>
	<td>$ovol</td>
	<td>$count</td>
	<td>$insdt</td>
	<td>
			<form>
			<input type='button' value='Download' onClick=\"window.location.href='charts/$name-$id.xls'\" />
			</form>
		</td>	
	</tr>";
}

print '
	</tbody>
	</table>

</body>
</html>';

# format julian date for viewing
sub pretty_date {
	my ($date)= @_;
	$date =~ m/(\d{4})(\d{2})(\d{2})/;
	return "$1/$2/$3";
	
}

sub compareTimes {
	my ($otime, $insert_dt) = @_;
	# feed offset converted to prev/cur/next hh:mm
	# and sql datetime in y-m-d hh:mm
	my $date = $dbdate;
	if ($otime =~ m/prev/) {
		($date,,) = FeedForecast::decrement_day($dbdate);	
	}
	elsif ($otime =~ m/next/) {
		$dbdate =~ m/(\d{4})(\d{2})(\d{2})/;
		my $tmpdate = "$1-$2-$3";
		$date = FeedForecast::increment_day($tmpdate);
	}
	
	$otime =~ m/(\d+:\d+)/;
	$otime = "$date $1";
	
	my $forecasted = ParseDate($otime);
	$forecasted = DateCalc($forecasted, 'in 30 minutes');
	my $recvd = ParseDate($insert_dt);
	
	return Date_Cmp($forecasted, $recvd);
}

# function to calculate time of day from minute offset
sub calcTime {
	my ($offset) = @_; 
	my $day = "prev";
	if ($offset >= 2880) {
		$offset -= 2880;
		$day = "next";
	}
	elsif ($offset >= 1440) {
		$offset -= 1440;
		$day = "curr";
	}
	my $hours = int($offset / 60);
	if ($hours > 24 || $hours < 0) {
		return "error";	
	}
	my $minutes = $offset - ($hours * 60);
	
	return sprintf("%s %u:%02u",$day,$hours,$minutes);
}