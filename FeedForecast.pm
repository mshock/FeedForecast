#!/usr/bin/perl

# config and general sub package
# for forecasting feeds with NN

package FeedForecast;

use strict;
use AppConfig qw(:argcount);
use Date::Calc qw(Add_Delta_Days Day_of_Week);

# read configs from this file
my $conf_file = 'FeedForecast.conf';


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

	# load config file (override with CLI args)
	$config->file($conf_file);

	# date range before/after current day for time offset
	$config->set('edaterange', 3);
	# percent of transactions for exchange to be considered complete when calculating initial metrics
	$config->set('calc_completion', .98);
	# date to start reading db from if no records found
	$config->set('date_begin_init', 20110122);
	# set log to text file
	$config->set('logging', 1);
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
	$config->set('cascade_path', "CascadeTest.exe");
	# flag to generate test data sets (required for shuffling data before training)
	$config->set('test_flag', 1);
	# shuffle training data flag
	$config->set('shuffle_flag', 1);
	# fraction of total data to use for testing
	$config->set('test_perc', .1);
	# directory for network definition files
	$config->set('nets_dir', 'nets');
	# directory of network execution exe
	$config->set('net_exe','TestNet.exe');
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
	$config->set('late_thresh', 10);
	# port to host application server on
	$config->set('serverport', 8888);
	$config->set('serverlog', 'logs/server.log');
	$config->set('serververbose', 1);
	

	
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


# calculate and format a SQL-usable datetime string
sub currtime {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);
	return sprintf("%02u/%02u/%u %02u:%02u:%02u", $mon + 1, $mday, $year + 1900, $hour, $min, $sec);
}