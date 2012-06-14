#! perl

# produce an excel graph of the score up to 2 days ago (assume the most recent 2 days are still completing)
# run once per day, on runnet.pl

use strict;
use DBI;
use Date::Manip qw(ParseDate Date_IsWorkDay);
use Spreadsheet::WriteExcel;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $chartdir = $config->chartdir();

my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");

# get all dates which the daemon has run
my $dates_q = $nndb->prepare("select distinct Date from DaemonLogs where 
						Date < CAST( FLOOR( CAST(GETDATE() AS FLOAT) ) AS DATETIME)");
$dates_q->execute();
my $dates_aref = $dates_q->fetchall_arrayref();
$dates_q->finish();

my $vol_aref = query_nndb("select Date,sum(VolumeScore)
		from DaemonLogs where
		VolumeScore<0
		group by Date
		union
		select Date,sum(VolumeScore)
		from DaemonLogs where
		VolumeScore>0
		group by Date");
		
my $time_aref = query_nndb("select Date,sum(TimeScore)
		from DaemonLogs where
		TimeScore<0
		group by Date
		union
		select Date,sum(TimeScore)
		from DaemonLogs where
		TimeScore>0
		group by Date");


# graph datasets
my $dates = [];
my $vol_neg = [];
my $vol_pos = [];
my $vol_tot = [];
my $time_neg = [];
my $time_pos = [];
my $time_tot = [];
my $tot_neg = [];
my $tot_pos = [];
my $tot_tot = [];

my %vol_data = ();
my %time_data = ();


add_data(\%vol_data, $vol_aref);
add_data(\%time_data, $time_aref);

my $num_dates = 0;
foreach my $date (@{$dates_aref}) {
	$date = @{$date}[0];
	$date =~ s/\s.*//;
	my $parsed_date = ParseDate($date);
	if (! Date_IsWorkDay($parsed_date)) {
		print "skipping weekend: $date\n";
		next;
	}
	$num_dates++;
	push @{$dates}, $date;
	
	push @{$vol_neg}, $vol_data{$date}{neg} || '0';
	push @{$vol_pos}, $vol_data{$date}{pos} || '0';
	push @{$vol_tot}, $vol_data{$date}{neg} + $vol_data{$date}{pos} || '0';
	
	push @{$time_neg}, $time_data{$date}{neg} || '0';
	push @{$time_pos}, $time_data{$date}{pos} || '0';
	push @{$time_tot}, $time_data{$date}{neg} + $time_data{$date}{pos} || '0';
	
	push @{$tot_neg}, $vol_data{$date}{neg} + $time_data{$date}{neg} || '0';
	push @{$tot_pos}, $vol_data{$date}{pos} + $time_data{$date}{pos} || '0';
	push @{$tot_tot}, $vol_data{$date}{neg} + $vol_data{$date}{pos} + $time_data{$date}{neg} + $time_data{$date}{pos} || '0';
}


my $workbook = Spreadsheet::WriteExcel->new("$chartdir/score_report.xls");
my $timechart = $workbook->add_chart( type => 'line', name => 'time chart');
my $volchart = $workbook->add_chart( type => 'line', name => 'volume chart');
my $totchart = $workbook->add_chart( type => 'line', name => 'total chart');
my $worksheet = $workbook->add_worksheet();

$worksheet->write('A1', [$dates]);
$worksheet->write('B1', [$vol_tot]);
$worksheet->write('C1', [$vol_pos]);
$worksheet->write('D1', [$vol_neg]);
$worksheet->write('E1', [$time_tot]);
$worksheet->write('F1', [$time_pos]);
$worksheet->write('G1', [$time_neg]);
$worksheet->write('H1', [$tot_tot]);
$worksheet->write('I1', [$tot_pos]);
$worksheet->write('J1', [$tot_neg]);

$timechart->add_series(
	name => 'Total',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$B\$1:\$B$num_dates"	
);
$timechart->add_series(
	name => 'Positive',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$C\$1:\$C$num_dates"	
);
$timechart->add_series(
	name => 'Negative',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$D\$1:\$D$num_dates"	
);

$volchart->add_series(
	name => 'Total',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$E\$1:\$E$num_dates"	
);
$volchart->add_series(
	name => 'Positive',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$F\$1:\$F$num_dates"	
);
$volchart->add_series(
	name => 'Negative',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$G\$1:\$G$num_dates"	
);

$totchart->add_series(
	name => 'Total',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$H\$1:\$H$num_dates"	
);
$totchart->add_series(
	name => 'Positive',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$I\$1:\$I$num_dates"	
);
$totchart->add_series(
	name => 'Negative',
	categories => "=Sheet1!\$A\$1:\$A$num_dates",
	values => "=Sheet1!\$J\$1:\$J$num_dates"	
);


# add a sum to its dataset
sub add_data {
	my ($href, $aref) = @_;
	my @rows = @{$aref};
	
	foreach my $rowref (@rows) {
		my ($date, $sum) = @{$rowref};
		#print "$date $sum\n";
		$date =~ s/\s.*//;
		
		# positive case
		if ($sum > 0) {
			$href->{$date}->{pos} = $sum;
		}
		# negative case
		elsif ($sum < 0) {
			$href->{$date}->{neg} = $sum;
		}
	}
}

sub query_nndb {
	my ($query) = @_;
	my $q = $nndb->prepare($query);
	$q->execute();
	my $aref = $q->fetchall_arrayref();
	$q->finish();
	return $aref;
}
