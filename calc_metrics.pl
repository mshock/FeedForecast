#!/usr/bin/perl -w

# calculate and update historical data

my $start_time = time;

use strict;
use File::stat;
use Time::localtime;
use DBI;
use Date::Calc qw(Add_Delta_Days);
use Parallel::ForkManager;
use Time::Duration;
use FeedForecast;

# load config variables
my $config = FeedForecast::loadConfig();

# logfile
my $logfile = $config->cm_logfile();

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

if (! -d "logs") {
	print "no log dir found, creating one...\n";
	mkdir("logs") or die("could not create log dir: $!\n");
	mkdir("logs/exchrun") or die("could not create log/exchrun dir: $!\n");
}

open (LOGFILE, '>', $logfile);


# create a ForkManager
my $forkManager = new Parallel::ForkManager($config->cm_procs());

# iterate over all feeds
foreach (@feeds) {
	$forkManager->start and next;
	chomp;
	
	`perl $_`;
	
	$forkManager->finish;
}

$forkManager->wait_all_children;

wout(1,"finished");

print LOGFILE FeedForecast::currtime() . "\tfinished in " . duration(time - $start_time) . "\n";

close LOGFILE;

# build the training files
exec(sprintf("perl \"%s\"", $config->buildtraining_loc()));