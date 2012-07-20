#!/usr/bin/perl -w

# daemon for NN validation
# runs all NNs to generate forecasting data
# should be added to windows task scheduler (AT) at daily intervals

use strict;
use DBI;
use Date::Calc qw(Add_Delta_Days Day_of_Week);
use Symbol 'delete_package';
use Parallel::ForkManager;
use Scalar::Util qw(looks_like_number);
use FeedForecast;

my $config = FeedForecast::loadConfig();

print FeedForecast::currtime() . "\trunning nets\n\n";

my @feeds = FeedForecast::get_feeds();

foreach my $feed (@feeds) {
	#"feed_config::$feed"->require;
	#eval "use feed_config::$feed ";
	require "feed_config/$feed.pm";
	import $feed qw(run_nets);
	#$feed->import(qw(init run_nets));
	
	init();
	# build training for this feed
	run_nets();
	
	# clean up feed package
	delete_package($feed);
}

print FeedForecast::currtime() . "\tdone.\n";

print "updating score report...\n";
`perl score_report.pl`;
print "done.\n";


# read the exchange log from the training file data script
sub load_exchanges {
	my $exchlog = $config->exchange_log();
	open (EXCH, $exchlog);
	my @exchanges = <EXCH>;
	close EXCH;
	my %exchash = ();
	for (@exchanges) {
		chomp;
		my ($code, $name) = split ',';
		$exchash{$name} = $code;
	}
	return %exchash;
}

