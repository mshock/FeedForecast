#/usr/bin/perl

# script to display webapp html

use strict;
use DBI;
use Getopt::Std;
use URI::Escape;
use Date::Manip qw(ParseDate Date_Cmp DateCalc);
use FeedForecast;

use vars qw($opt_d $opt_l $opt_s $opt_t $opt_o);

getopts('d:ls:t:o:');

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
		$search = "and r.name_ = '$opt_s'";
		$country_selected = 'selected=true';
	}
}

# sort params
my ($sort_sql, $sort_index) = get_sort_sql($opt_o);
# make sorted column header bold
my @colsort;
foreach (0..10) {
	$colsort[$_] = $sort_index == $_ ? 'headersort' : 'headerunsort';
}



my $printdate = pretty_date($dbdate);
if ($dbdate == FeedForecast::calc_date()) {
	$printdate = FeedForecast::currtime();
}


my $result = $nndb->prepare("select e.ExchName, nr.ExchID, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, CurrentVolume, State, dl.InsDateTime, r.name_
				from NetResults nr 
					join DaemonLogs dl 
						on nr.Date = dl.Date and nr.ExchID = dl.ExchID
					join exchanges e
						on nr.ExchID = e.ExchIntCode
					join regions r
						on r.region = e.exchctrycode
				where 
				 nr.Date = '$dbdate' and
				 r.regcodetypeid = 1
				 $search
				 order by $sort_sql");
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
	<form method='GET'>
	
	<table cellspacing='0' width='100%'>
		<thead>
		<tr>
			<th colspan='12' ><h2>Market Date $printdate GMT</h2></th>
		</tr>
		<tr>
			<th colspan='2'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
			<th colspan='7'>
				<input type='submit' value='search'/> 
				<input type='reset' value='reset' onclick='parent.location=\"?\"'/>
				<input type='text' name='date' value='$pretty_date' />
				<input type='text' name='search' value='$opt_s'/>
				<select name='search_type'>
					<option value='exchange' $exch_selected >Exchange</option>
					<option value='country' $country_selected >Country</option>
				</select>
				|
				<input type='checkbox' name='show_late' value='true' $late_checked/> Show Late
				
				
			</th>
			<th colspan='3'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
		</tr>
		<tr>
			<th><input type='submit' class='$colsort[0]' name='sort' value='Exchange Name' /></th>
			<th><input type='submit' class='$colsort[1]' name='sort' value='Country' /></th>
			<th><input type='submit' class='$colsort[2]' name='sort' value='Exchange ID' /></th>
			<th><input type='submit' class='$colsort[3]' name='sort' value='Last Day Recvd' /></th>
			<th><input type='submit' class='$colsort[4]' name='sort' value='Last DoM' /></th>
			<th><input type='submit' class='$colsort[5]' name='sort' value='Last DoW' /></th>
			<th><input type='submit' class='$colsort[6]' name='sort' value='Last Volume' /></th>
			<th><input type='submit' class='$colsort[7]' name='sort' value='ETA' /></th>
			<th><input type='submit' class='$colsort[8]' name='sort' value='Expected Volume' /></th>
			<th><input type='submit' class='$colsort[9]' name='sort' value='Actual Volume' /></th>
			<th><input type='submit' class='$colsort[10]' name='sort' value='Time Recvd' /></th>
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
	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt, $country) = @{$row};
	
	my $otime = FeedForecast::calcTime($ooffset);
	
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
	
	my $itime = FeedForecast::calcTime($ioffset);
	
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
	<td>$country</td>
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
	</form>
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
	$forecasted = DateCalc($forecasted, 'in ' . $config->show_late() . ' minutes');
	my $recvd = ParseDate($insert_dt);
	
	return Date_Cmp($forecasted, $recvd);
}

sub get_sort_sql {
	my ($sort_by) = @_;
	
	my %col_hash = (
		'Exchange Name' => ['e.ExchName', 0],
		'Country' => ['r.name_', 1],
		'Exchange ID' => ['nr.ExchID',2],
		'Last Day Recvd' => ['InputOffset',3],
		'Last DoM' => ['DayofMonth',4],
		'Last DoW' => ['DayofWeek',5],
		'Last Volume' => ['InputVolume',6],
		'ETA' => ['OutputOffset', 7],
		'Expected Volume' => ['OutputVolume',8],
		'Actual Volume' => ['CurrentVolume',9],
		'Time Recvd' => ['dl.InsDateTime',10],
	);
	
	# default to country if not set
	if (!$sort_by) {
		$sort_by = 'Country';
	} 
	
	# lookup sort_by table name
	my ($sql, $col) = @{$col_hash{$sort_by}};
	
	# return sort sql
	return ($sql . ' ASC', $col);	
}
