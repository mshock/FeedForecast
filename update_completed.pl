#!perl

# get the filedate,filenum and buildnums of completed exchanges
# forked by update daemon when exchange is marked complete for the first time

use strict;
use DBI;
use Date::Manip qw(ParseDate Date_Cmp DateCalc);
use Parallel::ForkManager;
use Fcntl qw(:flock);
use FeedForecast;


my $marketdate = $ARGV[0];

open(LOCK, '>', "logs/$marketdate.lock");
flock(LOCK, LOCK_EX | LOCK_NB) or die "lockfile is locked\n";
my @lock = <LOCK>;
print "lock acquired\n";
print LOCK $$;

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
	my $prod1 = DBI->connect($config->prod1_connection()) or die("Couldn't connect to prod1: $!\n");  
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	my $old_query = "select top 1 [ExecutionDateTime],[MakeUpdateRunDate],[MakeUpdateSequence], [BuildNumber], count([...]) as c
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
			order by c DESC";
	
	my $new_query2 = "select top 1 [ExecutionDateTime],Filedate,filenum,BuildNumber, count([...]) as c
				from
				[TQALic].dbo.[PackageQueue] q with (NOLOCK)
				join
				[172.22.85.170].DataIngestionInfrastructure.dbo.MakeUpdateInfo i with (NOLOCK)
				on i.DISTransactionNumber = q.TransactionNumber
				join
				[172.22.85.170].[DataStream2_Change].[dbo].[DS2PrimQtPrc] d with (NOLOCK)
				on q.TransactionNumber = d.[...]
				
				where
				 DataFeedId = 'DS2_EQIND_DAILY'
				 and RefPrcTypCode = 1 
				 and MarketDate = ?
				 and ExchIntCode = ?
				group by ExecutionDateTime, FileDate, FileNum, BuildNumber
				order by c DESC";
	
	my $new_query1 = "select top 1 [ExecutionDateTime],Filedate,filenum,BuildNumber, count([...]) as c
				from DataIngestionInfrastructure.dbo.MakeUpdateInfo i
				join 
				[172.22.85.164].[TQALic].dbo.[PackageQueue] q
				on i.DISTransactionNumber = q.TransactionNumber
				join
				[DataStream2_Change].[dbo].[DS2PrimQtPrc] d
				on q.TransactionNumber = d.[...]
				--with (NOLOCK)
				where
				 DataFeedId = 'DS2_EQIND_DAILY'
				 and RefPrcTypCode = 1 
				 and MarketDate = ?
				 and ExchIntCode = ?
				group by ExecutionDateTime, FileDate, FileNum, BuildNumber
				order by c DESC
	";
	my $info_query = $prod1->prepare( $new_query2
	) or warn "could not prepare new query\n";
	
	my ($exchid, $insdt, $curvol) = @{$exchange};
	
	#open LOG, '>>update_completed.log';
	#print LOG "$new_query\nmd:$marketdate\nexchid:$exchid\n";
	#close LOG;
	
	print "checking $exchid, $insdt\n";
	
	$info_query->execute($marketdate, $exchid) or warn "info query failed in update_completed\n";
	my @results = $info_query->fetchrow_array();		
	$info_query->finish();
	$prod1->disconnect();
	
	my ($exec_time, $filedate, $filenum, $buildnum, $count) = @results;
	#2013-01-22 11:06:54.863,	2013-01-21 00:00:00.000,	23,	11,	219
	print " $exchid results: @results\n";
	
	# skip to next if no result
	if (!$exec_time) {
		# exit child process
		$forkManager->finish;
		next;
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
		next;
	}
	
	my $update_query = $nndb->prepare(
	"update DaemonLogs 
	set BuildNumber = ?,
	FileNumber = ?,
	FileDate = $filedate,
	InsDateTime = ?
	where ExchID = ?
	and Date = '$marketdate'"
	);
	
	print "$exchid: $buildnum $filenum $filedate\n";
	
	# if made it this far, run update
	$update_query->execute($buildnum,$filenum,$exec_time,$exchid);
	$update_query->finish();
	
	$nndb->disconnect();
	
	print "updated $exchid on $marketdate with @results\n";
	
	# fork an async process to update NN score
	system 1, "perl update_score.pl $marketdate $exchid";
	
	# exit child process
	$forkManager->finish;
}

$forkManager->wait_all_children;

print LOCK "\nunlocked";
flock(LOCK, LOCK_UN) or die "could not release lock...\n$!\n";
close LOCK;

print "finished $marketdate";

