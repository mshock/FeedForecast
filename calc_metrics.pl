#!/usr/bin/perl -w

# calculate and update historical data

my $start_time = time;

use strict;
use Symbol 'delete_package';
use DBI;
use Date::Calc qw(Add_Delta_Days);
use Parallel::ForkManager;
use Time::Duration;
use FeedForecast;

# load config variables
my $config = FeedForecast::loadConfig();

# logfile
my $logfile = $config->cm_logfile();


if (! -d "logs") {
	print "no log dir found, creating one...\n";
	mkdir("logs") or die("could not create log dir: $!\n");
	mkdir("logs/exchrun") or die("could not create log/exchrun dir: $!\n");
}

open (LOGFILE, '>', $logfile);

# create a ForkManager
my $forkManager = new Parallel::ForkManager($config->cm_procs());

my @feeds = FeedForecast::get_feeds();

# iterate over all feeds
foreach my $feed (@feeds) {
	$forkManager->start and next;
	
	require "feed_config/$feed.pm";
	import $feed qw(calc_metrics);
	
	# run initialization routines for this feed
	init();
	# calculate and insert historical metrics for this feed
	calc_metrics();
	# clean up package data
	delete_package($feed);
	
	
	$forkManager->finish;
}

$forkManager->wait_all_children;

wout(1,"finished");

print LOGFILE FeedForecast::currtime() . "\tfinished in " . duration(time - $start_time) . "\n";

close LOGFILE;

# build the training files
exec(sprintf("perl \"%s\"", $config->buildtraining_loc()));