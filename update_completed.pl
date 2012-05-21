#!perl

# get the filedate,filenum and buildnums of completed exchanges
# forked by update daemon when exchange is marked complete for the first time

use strict;
use DBI;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	

my $marketdate = $ARGV[0];
my $exchange = $ARGV[1];


my $info_query = $ds2_c->prepare(
"select top 1 [...], [ExecutionDateTime],[MakeUpdateRunDate],[MakeUpdateSequence], [BuildNumber]
		from [DataStream2_Change].[dbo].[DS2PrimQtPrc],
		[NTCP-DIS1].disforlegacy.dbo.makeupdateinfo
		with (NOLOCK)
		where 
		RefPrcTypCode = 1 
		and [...] = [DISTransactionNumber]
		and MarketDate = ?
		and DataFeedId = 'DS2_EQIND_DAILY'
		and ExchIntCode = ?
		order by ExecutionDateTime DESC");

$info_query->execute($marketdate, $exchange);
my @results = $info_query->fetchrow_array();
$info_query->finish();
$ds2_c->disconnect();

if (!$results[2]) {
	$results[2] = 'NULL';
}
if (!$results[4]) {
	$results[4] = 'NULL';
}

my $update_query = $nndb->prepare(
"update DaemonLogs 
set BuildNumber = ?,
FileNumber = ?,
FileDate = '$results[2]'
where ExchID = ?
and Date = '$marketdate'"
);

$update_query->execute($results[4],$results[3],$exchange);

print "updated $exchange on $marketdate with @results\n";
