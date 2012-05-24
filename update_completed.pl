#!perl

# get the filedate,filenum and buildnums of completed exchanges
# forked by update daemon when exchange is marked complete for the first time

use strict;
use DBI;
use Date::Manip qw(ParseDate Date_Cmp DateCalc);
use Parallel::ForkManager;
use FeedForecast;


my $marketdate = $ARGV[0];

open(LOCK, '<', "logs/$marketdate.lock");
my @lock = <LOCK>;
close LOCK;
if ($lock[0] =~ /locked/) {
	print "lockfile is locked, exiting\n";
	exit;
}
else {
	print "locking lockfile\n";
	open(LOCK, '>', "logs/$marketdate.lock");
	print LOCK 'locked';
	close LOCK;
}



my $config = FeedForecast::loadConfig();




print "starting $marketdate\n";

my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");

my $check_completed = $nndb->prepare("select ExchID,insdatetime,CurrentVolume from 
				DaemonLogs where
				Date = '$marketdate' and
				State = 'recv' and
				(BuildNumber is null or FileNumber is null or FileDate is null)");

$check_completed->execute();
my $incomplete = $check_completed->fetchall_arrayref();
$check_completed->finish();
$nndb->disconnect();

# create a ForkManager to manage forking
my $forkManager = new Parallel::ForkManager($config->update_procs());
foreach my $exchange (@{$incomplete}) {
	# fork a new process if needed
	$forkManager->start and next;
	
	# create database stuff, due to fork
	my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	my $info_query = $ds2_c->prepare(
	"select top 1 [ExecutionDateTime],[MakeUpdateRunDate],[MakeUpdateSequence], [BuildNumber], count([...]) as c
			from [DataStream2_Change].[dbo].[DS2PrimQtPrc],
			[NTCP-DIS1].disforlegacy.dbo.makeupdateinfo
			with (NOLOCK)
			where 
			RefPrcTypCode = 1 
			and [...] = [DISTransactionNumber]
			and MarketDate = ?
			and DataFeedId = 'DS2_EQIND_DAILY'
			and ExchIntCode = ?
			group by [ExecutionDateTime],[MakeUpdateRunDate],[MakeUpdateSequence], [BuildNumber]
			order by c DESC");
	
	
	my ($exchid, $insdt, $curvol) = @{$exchange};
	
	print "checking $exchid, $insdt\n";
	
	$info_query->execute($marketdate, $exchid);
	my @results = $info_query->fetchrow_array();		
	$info_query->finish();
	$ds2_c->disconnect();
	
	my ($exec_time, $filedate, $filenum, $buildnum, $count) = @results;
	
	print " $exchid results: @results\n";
	
	# skip to next if no result
	if (!$exec_time) {
		# exit child process
		$forkManager->finish;
	}
	# if makeupdate hasn't been run, null values
	# if makeupdate hasn't been run, null values
	$filedate = $filedate ? "'$filedate'":'NULL';
	$filenum = $filenum ? $filenum :undef;
	
	# check if query has executed after exchange was marked as received
	# also test the volume recvd to make sure this isn't a false negative
	my $parsed_exec = ParseDate($exec_time);
	$parsed_exec = DateCalc($parsed_exec, 'in '. $config->update_window() .  ' minutes');
	my $parsed_insdt = ParseDate($insdt);
	if((Date_Cmp($parsed_exec, $parsed_insdt) != 1) && ($curvol * .25 > $count)) {
		print "$exchid date_cmp failed ($exec_time << $insdt) && ($curvol * .25 > $count) must be stale\n";
		# exit child process
		$forkManager->finish;
	}
	
	my $update_query = $nndb->prepare(
	"update DaemonLogs 
	set BuildNumber = ?,
	FileNumber = ?,
	FileDate = $filedate
	where ExchID = ?
	and Date = '$marketdate'"
	);
	
	print "$exchid: $buildnum $filenum $filedate\n";
	
	# if made it this far, run update
	$update_query->execute($buildnum,$filenum,$exchid);
	
	# update the time received with the executiontime, which is more accurate 
	
	
	$update_query->finish();
	$nndb->disconnect();
	
	print "updated $exchid on $marketdate with @results\n";
	# exit child process
	$forkManager->finish;
}

$forkManager->wait_all_children;

open(LOCK, '>', "logs/$marketdate.lock");
print LOCK '';
close LOCK;

print "finished $marketdate";

