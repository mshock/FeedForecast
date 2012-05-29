#/usr/bin/perl

# script to display webapp html

use strict;
use DBI;
use Getopt::Std;
use URI::Escape;
use Date::Manip qw(ParseDate Date_Cmp DateCalc UnixDate Date_ConvTZ);
use FeedForecast;

use vars qw($opt_d $opt_l $opt_s $opt_t $opt_o $opt_i $opt_z);

getopts('d:ls:t:o:iz:');

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
my $late_checked = $opt_l ? 'checked=true' : '';
# display only incomplete
my $inc_checked = $opt_i ? 'checked=true' : '';

# search params
my $search = '';
my $exch_selected = '';
my $country_selected = '';
if ($opt_s && $opt_t) {
	if ($opt_t eq 'exchange') {
		$search = "and nr.ExchName LIKE '%$opt_s%'";
		$exch_selected = 'selected=true'; 
	}
	elsif ($opt_t eq 'country') {
		$search = "and r.name_ LIKE '%$opt_s%'";
		$country_selected = 'selected=true';
	}
}

my $timezone = $opt_z ? $opt_z : 'GMT';
my $ist_selected = '';
my $cst_selected = '';
my $gmt_selected = '';
if ($timezone eq 'CST') {
	$cst_selected = 'selected=true';
}
elsif ($timezone eq 'IST') {
	$ist_selected = 'selected=true';
}

# sort params
my ($sort_sql, $sort_index) = get_sort_sql($opt_o);
# make sorted column header bold
my @colsort;
foreach (0..13) {
	$colsort[$_] = $sort_index == $_ ? 'headersort' : 'headerunsort';
}



my $printdate = pretty_date($dbdate);
my $headerdate = $printdate;
my $headertime = "&nbsp&nbsp&nbsp|&nbsp&nbsp&nbsp";
my $header_control = "<select name='timezone' onchange='this.form.submit()'>
					<option value='GMT' $gmt_selected >GMT</option>
					<option value='CST' $cst_selected >CST</option>
					<option value='IST' $ist_selected >IST</option>
				</select>";
if ($dbdate == FeedForecast::calc_date()) {
	$printdate = FeedForecast::currtime();
	$printdate =~ /(\d+).(\d+).(\d+)\s+(.*)/;
	$headerdate = "$1/$2/$3";
	
		if ($timezone eq 'GMT') {
			$headertime .= $4;
		}	
		elsif ($timezone eq 'CST') {
			my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
			$headertime .= sprintf("%02u:%02u:%02u", $hour, $min, $sec);
		}
		elsif ($timezone eq 'IST') {
			my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time + 3600 * 5.5);
			$headertime .= sprintf("%02u:%02u:%02u", $hour, $min, $sec);
		}
	
}

# get scores for header
my @scores = get_scores();

$headertime .= $header_control;

my $result = $nndb->prepare("select e.ExchName, nr.ExchID, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, CurrentVolume, State, dl.InsDateTime, r.name_, e.ExchCtryCode, dl.BuildNumber, dl.FileNumber, dl.FileDate
				from NetResults nr 
					join DaemonLogs dl 
						on nr.Date = dl.Date and nr.ExchID = dl.ExchID
					join exchanges e
						on nr.ExchID = e.ExchIntCode
					join regions r
						on r.region = e.exchctrycode
				where 
				 nr.Date = '$dbdate'
				 $search
				 order by $sort_sql");
$result->execute();

$dbdate =~ m/(\d{4})(\d\d)(\d\d)/;
my $pretty_date = sprintf("%u/%u/%u", $2, $3, $1);
my $nextdate = FeedForecast::increment_day("$1-$2-$3");
my ($prevdate, $trash1, $trash2) = FeedForecast::decrement_day($dbdate);
my ($ndate,$pdate,$cdate) = format_daterange($nextdate,$prevdate,$dbdate);


my $header_hover = 'title=\'click to sort\'';

# print html header
print_header();

# print table header
print_thead(@scores);

# get all the holidays for today
$pretty_date =~ /(\d+).(\d+).(\d+)/;
my %holidays = FeedForecast::get_holidays(sprintf("%u-%02u-%02u",$3,$1,$2));

# loop over exchange array and print to table
my (@error,@late,@wait,@recv);
while(my @row = $result->fetchrow_array()) {

	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt, $country, $ctrycode) = @row;
	
	# check if this exchange is on holiday
	if (exists $holidays{$ctrycode} && !($state eq 'recv')) {
		push(@error,[@row]);
		next;
	}
	
	if ($state eq "recv") {
		push(@recv,[@row]);
	}
	elsif ($state eq "late") {
		push(@late,[@row]);
	}
	elsif ($state eq "wait") {
		push(@wait,[@row]);
	}
}

# only get the recv'd rows if we're showing recv'd but late (checkbox)
my @rows = $opt_l ? (@late, @recv) : (@late, @wait, @recv, @error);
my $even_odd = 0;
my $eo = '';
my $border_prev = 0;
foreach my $row (@rows) {
	my ($name, $id, $ioffset, $dom, $dow, $ivol, $ooffset, $ovol, $count, $state, $insdt, $country, $ctrycode, $buildnum, $filenum, $filedate) = @{$row};
	
	my $ootime = FeedForecast::calcTime($ooffset);
	
	my $border_class1 = '';
	my $border_class2 = '';
	my $style = '';
	# if showing late compare times to find late 
	if ($opt_l && $state eq 'recv') {
		next if (FeedForecast::compareTimes($ootime, $insdt, $dbdate) != -1);
	}
	# if showing incomplete, hide recv'd
	elsif ($opt_i && ($state eq 'recv' || $state eq 'error')) {
		next;
	}
	# highlight recvd late exchanges with red border
	elsif ($state eq 'recv' && FeedForecast::compareTimes($ootime, $insdt, $dbdate) == -1) {
		$border_class1 = 'lateborder1';
		# i don't like how doubled up borders look...
		if (!$border_prev) {
			$border_class2 = 'lateborder2';
			$border_prev = 1;
		}
		else {
			$border_class2 = 'lateborder3';
		}
	}
	else {
		
		$border_prev = 0;
			
	}
	
	my $otime = FeedForecast::calcTime($ooffset, $timezone);
	
	
	if ($even_odd++ % 2) {
		$eo = 'odd';
	}
	else {
		$eo = 'even';
	}
	
	my $itime = FeedForecast::calcTime($ioffset, $timezone);
	
	$insdt =~ s/:\d\d\..*//;
	$insdt =~ s/0(\d:)/$1/;
	if (!$count) {
		$count = '---';
	}
	if (!$buildnum) {
		$buildnum = '---';
		$filedate = '---';
		$filenum = '---';
	}
	elsif ($filedate) {
		$filedate =~ s/\s.*//;
	}
	else {
		$filedate = '---';
		$filenum = '---';
	}
	
	if (!($state eq 'recv')) {
		$insdt = '---';
	}
	elsif ($timezone ne 'GMT') {
		my $tz;
		$tz = 'America/Chicago' if $timezone eq 'CST';
		$tz = 'Indian/Cocos' if $timezone eq 'IST';
		
		my $parsed_date = ParseDate($insdt);
		$parsed_date = Date_ConvTZ($parsed_date, 'Europe/London', $tz);
		$insdt = UnixDate($parsed_date, "%Y-%m-%d %H:%M");
	}
	
	
	# add dates for curr/next/prev
	#$itime =~ s/curr/$cdate/;
	#$itime =~ s/next/$ndate/;
	#$itime =~ s/prev/$pdate/;
	
	$otime =~ s/curr/$cdate/;
	$otime =~ s/next/$ndate/;
	$otime =~ s/prev/$pdate/;
	
	
	# add holiday hover and state
	my $holiday = '';
	if (exists $holidays{$ctrycode}) {
		my ($hol_name, $hol_type) = @{$holidays{$ctrycode}};
		$holiday = "title=\"Holiday Name: $hol_name\nHoliday Type: $hol_type\"";
		$state = 'error';
	}
	
	# set background accordingly
	my $row_class = $state . '_' . $eo;
	
	print "<tr class='$border_class1 $border_class2 $row_class' $holiday>
	<td ><span title='Exchange Name'>$name [$id]</span></td>
	<td ><span title='Country/Region'>$country</span></td>
	<td ><span title='Last Day Recvd ($timezone)'>$itime</span></td>
	<td ><span title='Last Volume'>$ivol</span></td>
	<td ><span title='ETA ($timezone)'>$otime</span></td>
	<td ><span title='Expected Volume'>$ovol</span></td>
	<td ><span title='Actual Volume'>$count</span></td>
	<td ><span title='Time Recvd ($timezone)'>$insdt</span></td>
	<td ><span title='Build Number'>$buildnum</span></td>
	<td ><span title='File Number'>$filenum</span></td>
	<td ><span title='File Date'>$filedate</span></td>
	<td >
			<form>
			<input type='button' value='Download' onClick=\"window.location.href='charts/$name-$id.xls'\" title='Download History Graph' />
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

sub get_sort_sql {
	my ($sort_by) = @_;
	
	my %col_hash = (
		'Exchange Name' => ['e.ExchName', 0],
		'Country' => ['r.name_', 1],
		'Exchange ID' => ['nr.ExchID',2],
		'Last Day Rec' => ['InputOffset',3],
		'Last DoM' => ['DayofMonth',4],
		'Last DoW' => ['DayofWeek',5],
		'Last Vol' => ['InputVolume',6],
		'ETA' => ['OutputOffset', 7],
		'Expected Vol' => ['OutputVolume',8],
		'Actual Vol' => ['CurrentVolume',9],
		'Time Rec' => ['dl.InsDateTime',10],
		'Build #' => ['dl.BuildNumber', 11],
		'File #' => ['dl.FileNumber', 12],
		'File Date' => ['dl.FileDate', 13]
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

sub format_daterange {
	return map {$_ =~ /\d{4}(\d{2})(\d{2})/; "$1/$2";} @_;
}

sub print_header {
	print "<html>
	<head>
	<meta http-equiv='refresh' content='300' > 
	<title>Monitor :: Market Date $headerdate</title>
	<link rel='stylesheet' type='text/css' href='styles.css' />
	</head>";
}

sub get_scores {
	my $negvolq = $nndb->prepare("select sum(VolumeScore) from DaemonLogs where 
		Date = '$dbdate' and VolumeScore < 0");
	my $posvolq = $nndb->prepare("select sum(VolumeScore) from DaemonLogs where 
		Date = '$dbdate' and VolumeScore > 0");
	my $negtimeq = $nndb->prepare("select sum(TimeScore) from DaemonLogs where 
		Date = '$dbdate' and TimeScore < 0");
	my $postimeq = $nndb->prepare("select sum(TimeScore) from DaemonLogs where 
		Date = '$dbdate' and TimeScore > 0");
	my $totalq = $nndb->prepare("select sum(TimeScore), sum(VolumeScore) from DaemonLogs where
		Date <= '$dbdate'");
	
	$negvolq->execute();
	my $negvol = ($negvolq->fetchrow_array())[0];
	$negvolq->finish();
	
	$posvolq->execute();
	my $posvol = ($posvolq->fetchrow_array())[0];
	$posvolq->finish();
	
	$negtimeq->execute();
	my $negtime = ($negtimeq->fetchrow_array())[0];
	$negtimeq->finish();
	
	$postimeq->execute();
	my $postime = ($postimeq->fetchrow_array())[0];
	$postimeq->finish();
	
	$totalq->execute();
	my $total = ($totalq->fetchrow_array())[0];
	$totalq->finish();
	
	my $totvol = $posvol+$negvol;
	my $tottime = $postime+$negtime;
	my $subtot = $totvol + $tottime;
	
	return ($posvol, $negvol, $totvol,
			$postime, $negtime, $tottime,
			$posvol+$postime, $negvol+$negtime, $total,
			$subtot);
}


sub print_thead {
	my ($volpos, $volneg, $voltot,
	 	$timepos, $timeneg, $timetot, 
	 	$totpos, $totneg, $tottot,
	 	$subtot)= @_;
	my $volcolor = $voltot >= 0 ? '33CC33' : 'FF0000';
	my $timecolor = $timetot >= 0 ? '33CC33' : 'FF0000';
	my $subtotcolor = $subtot >= 0 ? '33CC33' : 'FF0000';
	my $tottotcolor = $tottot >= 0 ? '33CC33' : 'FF0000';
	
	print "<body>
	<form method='GET'>
	
	<table class='score' cellspacing='0' title='NN Score'>
		<th>Score</th><th colspan=2><font color=$tottotcolor>$tottot</font></th>
		<tr>
			<td >Volume</td>
			<td>Time</td>
			<td>Total</td>
		</tr>
		<tr>
			<td><font color=33CC33>+$volpos</font></td>
			<td><font color=33CC33>+$timepos</font></td>
			<td><font color=33CC33>+$totpos</font></td>
		</tr>
		<tr>
			<td><font color=FF0000>$volneg</font></td>
			<td><font color=FF0000>$timeneg</font></td>
			<td><font color=FF0000>$totneg</font></td>
		</tr>
		<tr>
			<td class='bottom_score'><font color=$volcolor>$voltot</font></td>
			<td class='bottom_score'><font color=$timecolor>$timetot</font></td>
			<td class='bottom_score'><font color=$subtotcolor>$subtot</font></td>
		</tr>
	</table>
	
	<table class='legend' cellspacing='0' title='legend'>
		<tr>
			<td class='late_even'>
			&nbsp
			</td>
			<td>
				Late
			</td>
		</tr>
		<tr>
			<td class='wait_even'>
			&nbsp
			</td>
			<td>
				Wait
			</td>
		</tr>
		<tr>
			<td class='recv_even'>
			&nbsp
			</td>
			<td>
				Received
			</td>
		</tr>
		<tr >
			<td class='laterecvex'>
			&nbsp
			</td>
			<td>
				Received Late
			</td>
		</tr>
		<tr>
			<td class='error_even'>
			&nbsp
			</td>
			<td>
				Holiday
			</td>
		</tr>
	</table>
	
	
	<table cellspacing='0' width='100%'>
		<thead>
		<tr>
			<th colspan='14' ><h2>Market Date $headerdate$headertime </h2></th>
		</tr>
		<tr>
			<th colspan='1'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
			<th colspan='8'>
				<input type='submit' value='search'/> 
				<input type='reset' value='reset' onclick='parent.location=\"?\"'/>
				<input type='text' name='date' value='$pretty_date' />
				<input type='text' name='search' value='$opt_s' title='Search'/>
				<select name='search_type'>
					<option value='exchange' $exch_selected >Exchange</option>
					<option value='country' $country_selected >Country</option>
				</select>
				<br />
				<input type='checkbox' name='show_late' value='true' $late_checked/> Show Late
				<input type='checkbox' name='show_incomplete' value='true' $inc_checked/> Show Incomplete
				<input type='button' value='Export' onClick=\"window.location.href='charts/report_$dbdate.xls'\" />
				
				
			</th>
			<th colspan='3'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
		</tr>
		<tr>
			<th><input type='submit' class='$colsort[0]' name='sort' value='Exchange Name' $header_hover /></th>
			<th><input type='submit' class='$colsort[1]' name='sort' value='Country' $header_hover /></th>
			<th><input type='submit' class='$colsort[3]' name='sort' value='Last Day Rec' $header_hover /></th>
			<th><input type='submit' class='$colsort[6]' name='sort' value='Last Vol' $header_hover /></th>
			<th><input type='submit' class='$colsort[7]' name='sort' value='ETA' $header_hover /></th>
			<th><input type='submit' class='$colsort[8]' name='sort' value='Expected Vol' $header_hover /></th>
			<th><input type='submit' class='$colsort[9]' name='sort' value='Actual Vol' $header_hover /></th>
			<th><input type='submit' class='$colsort[10]' name='sort' value='Time Rec' $header_hover /></th>
			<th><input type='submit' class='$colsort[11]' name='sort' value='Build #' $header_hover /></th>
			<th><input type='submit' class='$colsort[12]' name='sort' value='File #' $header_hover /></th>
			<th><input type='submit' class='$colsort[13]' name='sort' value='File Date' $header_hover /></th>
			<th><input type='submit' class='headerunsort' value='Graph' title='Download History Graph' /></th>
			
		</tr>
		</thead>
		<tbody>";
}