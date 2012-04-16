#/usr/bin/perl

# script to display webapp html

use strict;
use DBI;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $cur_page = $0;

#$connection = odbc_connect("Driver={SQL Server Native Client 10.0};Server=$server;Database=$DB;", $user, $pass) or die("failed to connect!");
my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n"); 

# get the date to look at
my $dbdate = FeedForecast::calc_date();
if ($ARGV[0] && $ARGV[0] =~ m/^\d{8}$/) {
	$dbdate = $ARGV[0];
}

my $printdate = pretty_date($dbdate);
if ($dbdate == FeedForecast::calc_date()) {
	$printdate = FeedForecast::currtime();
}


my $result = $nndb->prepare("select ExchName, nr.ExchID, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, CurrentVolume, State, dl.InsDateTime
				from NetResults nr join DaemonLogs dl 
				on nr.Date = dl.Date and nr.ExchID = dl.ExchID
				where 
				 nr.Date = '$dbdate'");
$result->execute();

$dbdate =~ m/(\d{4})(\d\d)(\d\d)/;
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
			<th colspan='6'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
			<th colspan='5'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
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

my @rows = (@error, @late, @wait, @recv);
my $even_odd = 0;
my $eo = '';
foreach my $row (@rows) { 
	if ($even_odd++ % 2) {
		$eo = 'odd';
	}
	else {
		$eo = 'even';
	}
	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt) = @{$row};
	
	my $itime = calcTime($ioffset);
	my $otime = calcTime($ooffset);
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