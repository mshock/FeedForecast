#!/usr/bin/perl -w

# build all training data from historical data

my $start_time = time;

use strict;
use DBI;
use Date::Calc qw(:all);
use Symbol 'delete_package';
use Spreadsheet::WriteExcel;
use File::Copy;
use Scalar::Util qw(looks_like_number);
use Parallel::ForkManager;
use Time::Duration;
use FeedForecast;

my $config = FeedForecast::loadConfig();


my $btlog = $config->bt_log();



open(LOG, '>', $btlog);
print LOG FeedForecast::currtime() . "\tbuildtraining routine started...\n";

my @feeds = FeedForecast::get_feeds();

foreach my $feed (@feeds) {
	require "feed_config/$feed.pm";
	import $feed qw(build_training);
	
	init();
	# build training for this feed
	build_training();
	
	# clean up feed package
	delete_package($feed);
}


print LOG FeedForecast::currtime() . "\tdone in " . duration(time - $start_time) . "\n";
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