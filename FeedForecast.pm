#!/usr/bin/perl

# config and general sub package
# for forecasting feeds with NN

package FeedForecast;

use strict;
use AppConfig qw(:argcount);
use Date::Calc qw(Add_Delta_Days Day_of_Week);
use Date::Manip qw(ParseDate Date_Cmp DateCalc UnixDate Date_ConvTZ);
use Net::SMTP;
use DBI;

1;

# load config variables for implementers
sub loadConfig {
	# set all default config values

	# create new appconfig for handling configuration variables
	my $config = AppConfig->new({
		CREATE => 1,	
		GLOBAL => {
	            DEFAULT  => "<undef>",
	            ARGCOUNT => ARGCOUNT_ONE,
	        },
	});

	$config->set('conf_file', 'FeedForecast.conf');
	# check if config file is actually one directory up, for running subscripts directly
	if (! -f $config->conf_file() && -f '../FeedForecast.conf') {
		$config->set('conf_file', '../FeedForecast.conf');
	}
	
	
	# load config file (override with CLI args)
	$config->file($config->conf_file());
	
	#$config->args();
	# date range before/after current day for time offset
	$config->set('edaterange', 3);
	# percent of transactions for exchange to be considered complete when calculating initial metrics
	$config->set('calc_completion', .98);
	# date to start reading db from if no records found
	$config->set('date_begin_init', 20110122);
	# set log to text file
	$config->set('logging', 0);
	# set to log to stdout (not recommended due to forks)
	$config->set('stdout_logging', 1);
	# calc_metrics logfile
	$config->set('cm_logfile','logs/calc_metrics.log');
	
	# keep addresses and credentials in non-version controlled .conf file
	# DISforLegacy info
	#$config->set('disfl_server','localhost');
	$config->set('disfl_database','DISForLegacy');
	#$config->set('disfl_user','');
	#$config->set('disfl_pass','');
	$config->set('disfl_connection',sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
		$config->disfl_database(),
		$config->disfl_server(),
		$config->disfl_user(),
		$config->disfl_pass()));
	# DS2_Change info
	#$config->set('ds2c_server','localhost');
	$config->set('ds2c_database','DataStream2_Change');
	#$config->set('ds2c_user','');
	#$config->set('ds2c_pass','');
	$config->set('ds2c_connection',sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
		$config->ds2c_database(),
		$config->ds2c_server(),
		$config->ds2c_user(),
		$config->ds2c_pass()));
	# DS2_Change info
	#$config->set('ds2_server','localhost');
	$config->set('ds2_database','DataStream2');
	#$config->set('ds2_user','');
	#$config->set('ds2_pass','');
	$config->set('ds2_connection',sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
		$config->ds2_database(),
		$config->ds2_server(),
		$config->ds2_user(),
		$config->ds2_pass()));
	# NNDB info
	#$config->set('nndb_server','localhost');
	$config->set('nndb_database','NNDB');
	#$config->set('nndb_user','');
	#$config->set('nndb_pass','');
	$config->set('nndb_connection',sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
		$config->nndb_database(),
		$config->nndb_server(),
		$config->nndb_user(),
		$config->nndb_pass()));
	
	# QADMASTER info
	$config->set('qadm_database', 'qai_master');
	$config->set('qadm_connection', sprintf("dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
		$config->qadm_database(),
		$config->qadm_server(),
		$config->qadm_user(),
		$config->qadm_pass()));
	
	# exchange log location (for calc_metrics)
	$config->set('exchange_log', 'logs/exchanges.log');
	# directory to save current run logs for exchanges to
	$config->set('exchmetrics_dir','logs/exchrun/');
	# buildtraining script location
	$config->set('buildtraining_loc','buildtraining.pl');
	# web server directory
	$config->set('chartdir', 'web/charts');
	# training log directory
	$config->set('bt_logdir','logs/training');
	# build training run logfile
	$config->set('bt_log','logs/bt.log');
	# path to cascade training network exe
	$config->set('cascade_path', "bin\\CascadeTest.exe");
	# flag to generate test data sets (required for shuffling data before training)
	$config->set('test_flag', 1);
	# shuffle training data flag
	$config->set('shuffle_flag', 1);
	# fraction of total data to use for testing
	$config->set('test_perc', .1);
	# directory for network definition files
	$config->set('nets_dir', 'nets');
	# directory of network execution exe
	$config->set('net_exe','bin\\TestNet.exe');
	# skip weekends flag
	$config->set('weekend_flag', 0);
	# log for last runnet (important - read by daemon)
	$config->set('runnet_log', 'logs/runnet.%s.log');
	# run update daemon only once flag
	$config->set('runonce',1);
	# frequency to run daemon (minutes)
	$config->set('freq', 5);
	# daemon run log file
	$config->set('daemon_log', 'logs/daemon.%s.log');
	# threshold before feed is marked complete (not exactly, is adjusted)
	$config->set('comp_thresh', .9);
	# pivot value for adjustment calculation
	$config->set('pivot', 100);
	# delta value for adjustment calculation
	$config->set('delta', .2);
	# amount of time a feed can be late (minutes) before marked late
	$config->set('late_thresh', 45);
	# port to host application server on
	$config->set('serverport', 8888);
	$config->set('serverlog', 'logs/server.log');
	$config->set('serververbose', 1);
	$config->set('training_iterations', 10);
	# number of processes to fork during training
	$config->set('training_procs', 3);
	# number of processes to fork during metric refresh (calc_metrics)
	$config->set('cm_procs', 10);
	# number of processes to fork while doing daily network run
	$config->set('runnet_procs', 5);
	$config->set('runnet_dryrun', 0);
	# number of minutes before showing as late on webapp
	$config->set('show_late', 45);
	$config->set('smtp_server', 'mailhub.tfn.com');
	$config->set('update_procs', 1);
	$config->set('update_window', 60);
	
	# load config file (override with CLI args)
	$config->file($config->conf_file());
	
	return $config;
}

# strip out non-ascii characters (database is in utf8???)
sub asciiify {
	my ($utf8) = @_;
	$utf8 =~ s/[^[:ascii:]]+//g;
	return $utf8;
}

# convert date in formate YYYY-MM-DD.* to YYYYMMDD
sub julianify {
	my ($date) = @_;
	$date =~ m/(\d{4})-(\d\d)-(\d\d)/;
	return $1 . $2 . $3;
}

# increment a dash format time and return it in Julian
sub increment_day {
	my ($date) = @_;
	$date =~ m/(\d{4})-(\d{2})-(\d{2})/;
	my ($y, $m, $d) = Add_Delta_Days($1, $2, $3, 1);
	return sprintf("%u%02u%02u", $y,$m,$d);
}

# decrement a julian date, return julian date with dom and dow
sub decrement_day {
	my ($date) = @_;
	$date =~ m/(\d{4})(\d{2})(\d{2})/;
	my ($y, $m, $d) = Add_Delta_Days($1, $2, $3, -1);
	my $wday = Day_of_Week($y, $m, $d);
	return (sprintf("%u%02u%02u", $y,$m,$d),$m,$wday);
}

# print format for current time in execution
sub exectime {
	my $ctime = time - $^T;
	return sprintf("[%02u:%02u:%02u]", $ctime / 3600 % 60, $ctime / 60 % 60, $ctime % 60);
}

# calculate and format today's date
sub calc_date {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);	
	return sprintf("%u%02u%02u", $year + 1900,$mon + 1,$mday);
}

# write to stdout
sub wout {
	my ($level, $line) = @_;
	my $tabs = "\t" x $level; 
	print exectime() . "$tabs$line\n";
	wlog($level, $line);
}

# function to calculate time of day from minute offset
sub calcTime {
	my ($offset, $tz) = @_;
	$tz = 'GMT' if !$tz;
	
	my %tzs = (
		'GMT' => 0,
		'CST' => -6,
		'IST' => 5.5,
	);
	
	# adjust for timezone	
	$offset += $tzs{$tz} * 60;
	
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

# calculate and format a SQL-usable datetime string
sub currtime {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);
	return sprintf("%02u/%02u/%u %02u:%02u:%02u", $mon + 1, $mday, $year + 1900, $hour, $min, $sec);
}

sub load_addresses {
	open(ADDS, '<', 'email_list.txt');
	my @addresses;
	while (<ADDS>) {
		chomp;
		push @addresses, $_ if !/#/;
	}
	close ADDS;
	return @addresses;
}

sub send_email {
	my $contentFileName_l = shift;
	my $subjectLine_l = shift;
	my $msgType_l = shift;
	
	my $smtpServer_l = shift;
	
	
	my $smtp = Net::SMTP->new($smtpServer_l);
	#my $smtp = Net::SMTP->new('qai-chi-mon01.qai.qaisoftware.com');
	
	$smtp->mail($ENV{USER});
	
	my @addList = load_addresses();
	foreach my $add (@addList)
	{
		$smtp->to($add);	
	} 
	
	$smtp->data();
	foreach my $add (@addList)
	{
		$smtp->datasend("To: $add\n");
	} 
	
	$smtp->datasend("From: DS_Monitor\n");
	$smtp->datasend("Subject: $subjectLine_l\n");
	$smtp->datasend("\n");
	
	if($msgType_l == 0) # use File
	{
		open(HANDLE, '<', $contentFileName_l);
		while ( <HANDLE> )
		{
			$smtp->datasend("$_");
		}
		close HANDLE;
	} elsif ($msgType_l == 1) # use Content File Name as MSG to send
	{
		$smtp->datasend("$contentFileName_l");
	}
	$smtp->dataend();

	$smtp->quit;
}

# return a hash of all holidays for a date
# hash will be indexed by country
# %hash{country} = (holiday_name,holiday_type)
#my %h = get_holidays('2012-05-11');
#for (keys %h) {
#	my @hol = @{$h{$_}};
#	print $hol[0].' '.$hol[1]. "\n";
#}
sub get_holidays {
	my ($date) = @_;
	my $config = loadConfig();
	my $qadm = DBI->connect($config->qadm_connection());
	my $get_holidays = $qadm->prepare(
		"SELECT IsoCtry, sde.HolType, sdi.Name
		FROM [qai_master].[dbo].[SDExchInfo_v] sde,
		[qai_master].[dbo].[SDDates_v] sdd,
		[qai_master].[dbo].[SDInfo_v] sdi
		where 
		sde.ExchCode = sdd.ExchCode and
		sdi.Code = sdd.Code and 
		sdd.Date_ = ? and
		sde.ExchCode != 0"		
	);
	
	$get_holidays->execute($date);
	my %holhash = ();
	while (my @row = $get_holidays->fetchrow_array()) {
		my ($country,$holtype,$holname) = @row;
		push @{$holhash{$country}}, ($holname, $holtype);  
	}
	
	$get_holidays->finish();
	$qadm->disconnect();
	
	return %holhash;
}


# compare an offset to a db datetime
# feed offset converted to prev/cur/next hh:mm
# and sql datetime in y-m-d hh:mm
sub compareTimes {
	my ($otime, $insert_dt, $date) = @_;
	
	my $config = loadConfig();
	
	if ($otime =~ m/prev/) {
		($date,,) = FeedForecast::decrement_day($date);	
	}
	elsif ($otime =~ m/next/) {
		$date =~ m/(\d{4})(\d{2})(\d{2})/;
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

# write to stdout
sub wout {
	my $config = loadConfig();
	if (!$config->stdout_logging()) {
		return;	
	}
	my ($level, $line) = @_;
	my $tabs = "\t" x $level; 
	print calc_date() . "$tabs$line\n";
	wlog($level, $line);
}

# write to log file
sub wlog {
	my $config = loadConfig();
	if ($config->logging()) {
		my ($level, $line) = @_;
		my $tabs = "\t" x $level;
		print LOGFILE calc_date() . "$tabs$line\n";
	}
}


# compare executiondatetime against marketdate
# return true if too far in the future/past
sub compedtmd {
	my ($edt, $md) = @_;
	my $config = loadConfig();
	# convert edt to julian
	$edt =~ m/(\d{4})-(\d\d)-(\d\d)/;
	$edt = $1.$2.$3;
	# add range to marketdate
	$md =~ m/(\d{4})(\d\d)(\d\d)/;
	my ($y,$m,$d) = Add_Delta_Days($1,$2,$3,$config->edaterange());
	$md = $y.$m.$d;
	if ($edt > $md) {
		return 1;
	}
	($y,$m,$d) = Add_Delta_Days($1,$2,$3,-$config->edaterange());
	$md = $y.$m.$d;
	if ($edt < $md) {
		return 1;
	}
	return 0;
}

# insert a row into the new database format
sub get_ins_query {
	my ($table, $cols) = @_;
	
	# construct a transaction for all the column codes
	my $query = '
		begin tran
			declare @rid uniqueidentifier
			set @rid = NEWID()
			';
	$query .= "insert into $table (code_id, row_id, value) values (?, \@rid,?\n" x $cols;
	
	$query .= 'commit tran
			go';
	return $query;
}

sub get_feeds {
	my $config = loadConfig();
	# get all feeds that need forecasting
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	my $feeds = $nndb->prepare('select desc_ from ds');
	$feeds->execute();
	my $feeds_aref = $feeds->fetchall_arrayref();
	$feeds->finish();
	$nndb->disconnect();
	
	my @feeds;
	foreach my $aref (@{$feeds_aref}) {
		push @feeds, @{$aref}[0];
	}
	return @feeds;
}
