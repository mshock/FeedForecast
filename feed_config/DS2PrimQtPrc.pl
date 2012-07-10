#perl 

# this is a configuration script for this feed
# will be imported, subs will be called

use strict;
use Exporter;
use lib '..';
use FeedForecast;

# export functions
our @ISA = qw(Exporter);
our @EXPORT = qw();


# load config variables
my $config = FeedForecast::loadConfig();


# some init stuff with exchanges

my $exchange_log = $config->exchange_log();

# check/build exchange log
load_exchanges();
open EXCH, '<', $exchange_log;
my @exchanges = <EXCH>;
close EXCH;


# split exchange query results
my ($exchcode, $exchname) = split(',', $_);


# 

open (TFILE, '>', $config->exchmetrics_dir() . "$exchname-$exchcode.log");
	
wout(1, "compiling data for $exchname [$exchcode]...");
	
	
my %finishtimes = calc_finish($exchcode);