#! /usr/bin/perl -w

use strict;
use DBI;
use Scalar::Util qw(looks_like_number);
use FeedForecast;
# update the status (waiting, recv'd, late) for each exchange
# also, once recv'd mark whether volume or time are close to forecast

my $config = FeedForecast::loadConfig();

# run once flag
my $runonce = $config->runonce();

my $runnet_log = $config->runnet_log();

my $daemon_log = $config->daemon_log();
# percentage of forecast that needs to be reached before feed is marked as complete
my $threshold = $config->comp_thresh();
# adjustment parameters
my $pivot = $config->pivot();
my $delta = $config->delta();

# amount of minutes to allow a feed to be late past forecast
my $to_thresh = $config->late_thresh();
# number of minutes between updates
my $freq = $config->freq(); 



print FeedForecast::currtime() . "\tstarting update daemon...\n";
my $first = 1;

do {
	# run update tasks at frequency
	if (!$first) {
		sleep($freq * 60);
	}
	else {
		$first = 0;
	}
	
	print FeedForecast::currtime() . "\tstarted update task\n";


	# create a ForkManager
	my $forkManager = new Parallel::ForkManager(2);
	
	my @feeds = FeedForecast::get_feeds();
	
	# iterate over all feeds
	foreach my $feed (@feeds) {
		$forkManager->start and next;
		
		require "feed_config/$feed.pm";
		import $feed qw(init updated);
		
		# run initialization routines for this feed
		init();
		# calculate and insert historical metrics for this feed
		updated();
		# clean up package data
		delete_package($feed);
		
		
		$forkManager->finish;
	}

} while (!$runonce);