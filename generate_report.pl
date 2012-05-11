#!perl

# create xml report in the format of the webapp
# called from update daemon

use strict;
use DBI;
use Spreadsheet::WriteExcel;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $chartdir = $config->chartdir();
my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n"); 

my $dbdate = $ARGV[0] ? $ARGV[0] : FeedForecast::calc_date();

# create Excel sheet
my $workbook = Spreadsheet::WriteExcel->new("$chartdir/report_$dbdate.xls");
my $worksheet = $workbook->add_worksheet();
my $form_late = $workbook->add_format(bg_color => 'red');
my $form_wait = $workbook->add_format(bg_color => 'yellow');
my $form_recv = $workbook->add_format(bg_color => 'green');
my $form_holi = $workbook->add_format(bg_color => 'blue');
my $head_color = $workbook->set_custom_color(42, '#DBD7D5');
my $form_head = $workbook->add_format(bg_color => $head_color);


my $result = $nndb->prepare("select e.ExchName, r.name_, nr.ExchID, InputOffset, DayofMonth, DayofWeek, InputVolume, OutputOffset, OutputVolume, CurrentVolume, dl.InsDateTime, State 
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
				 order by r.name_");

# initialize with headers
my @chartdata = ('Exchange Name',
				'Country',
				'Exchange ID',
				'Last Day Recvd',
				'Last DoM',
				'Last DoW',
				'Last Volume',
				'ETA',
				'Expected Volume',
				'Actual Volume',
				'Time Recvd',
				'Status'
);
$worksheet->write('A1', \@chartdata, $form_head);

# get holidays
$dbdate =~ /(\d{4})(\d\d)(\d\d)/;
my %holidays = FeedForecast::get_holidays("$1-$2-$3");

# write all rows in query to spreadsheet
$result->execute();
my $row_num = 1;
my $bg_color;
while(@chartdata = $result->fetchrow_array()) {
	# calculate actual times from offsets
	$chartdata[3] = FeedForecast::calcTime($chartdata[3]);
	$chartdata[7] = FeedForecast::calcTime($chartdata[7]);
	
	# set background color according to state
	if (exists $holidays{lc $chartdata[1]}) {
		$bg_color = $form_holi;
	}
	elsif ($chartdata[11] eq 'late') {
		$bg_color = $form_late;
	}
	elsif ($chartdata[11] eq 'recv') {
		$bg_color= $form_recv;
	}
	else {
		$bg_color= $form_wait;
	}
	
	# write row to spreadsheet
	$worksheet->write('A'.(++$row_num), \@chartdata, $bg_color);
}

$workbook->close();
$nndb->disconnect();