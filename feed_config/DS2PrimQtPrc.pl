#!perl 

# this is a configuration script for this feed
# will be imported, subs will be called

use strict;
use lib '..';
use FeedForecast;

# load config variables
my $config = FeedForecast::loadConfig();

# date to begin collecting training data
my $date_begin_init = $config->date_begin_init();

# some init stuff with exchanges
my $exchange_log = $config->exchange_log();

# initialization function
sub init {
	# check/build exchange log
	load_exchanges();
}


sub calc_metrics {
	
	open EXCH, '<', $exchange_log;
	my @exchanges = <EXCH>;
	close EXCH;
	
	# create a ForkManager
	my $forkManager = new Parallel::ForkManager($config->cm_procs());
	
	for (@exchanges) {
		$forkManager->start and next;
		# split exchange query results
		my ($exchcode, $exchname) = split(',', $_);
		
		wout(1, "compiling data for $exchname [$exchcode]...");
		
		# insert the metrics for this exchange into the database
		insert( calc_finish($exchcode), $exchname, $exchcode );
		$forkManager->finish;
	}
	$forkManager->wait_all_children;
}

sub insert {
	my (%finishtimes, $exchname, $exchcode) = @_;
	
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	open (TFILE, '>', $config->exchmetrics_dir() . "$exchname-$exchcode.log");
	
	wout(2, "writing times to database and log...");
	# prepare the insert query
	my $ins_query = FeedForecast::get_ins_query('history',5);
	my $nndb_insert = $nndb->prepare($ins_query);
	foreach my $date (sort (keys %finishtimes)) {
		print TFILE "$date\t$finishtimes{$date}{'complete'}\t$finishtimes{$date}{'count'}\n";
		my @ins_values = (
			207 => $date,
			205 => $exchcode,
			206 => $exchname,
			208 => $finishtimes{$date}{complete},
			209 => $finishtimes{$date}{count},
		);
		
		#print "inserting: $date,$exchcode,". asciiify($exchname).",$finishtimes{$date}{'complete'},$finishtimes{$date}{'count'}\n";
		
		$nndb_insert->execute(@ins_values);
	}
		
	close TFILE;
	
	$nndb->disconnect();
}

sub calc_finish {
	my ($exchcode) = @_;
	
	# open connections to DBs
	my $disfl = DBI->connect($config->disfl_connection()) or die("Couldn't connect to DISFL: $!\n");
	my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
	my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
	my $nndb = DBI->connect($config->nndb_connection()) or die("Couldn't connect to NNDB: $!\n");
	
	# get the date to start from from the nndb
	my $get_start = $nndb->prepare("select max(b.value) from history a
									inner join history b
									on a.row_id = b.row_id  
									where 
									a.code_id = 205
									and b.code_id = 207
									and a.value = ?");
	
	
	
	# query to load all transactions for infocodes within an exchange
	my $get_trans = $ds2_c->prepare("select [...], MarketDate, infocode 
		from [DataStream2_Change].[arc].[DS2PrimQtPrc] 
		where ExchIntCode = ? 
		and RefPrcTypCode = 1 
		and MarketDate > ?");
	
	
	# get execution time from marketinfo table
	my $get_marketinfo = $disfl->prepare("select ExecutionDateTime, BuildNumber,MakeUpdateSequence,MakeUpdateRunDate
		from [DISForLegacy].[dbo].[MakeUpdateInfo] with (NOLOCK)
		where DISTransactionNumber = ?
		and DataFeedId = 'DS2_EQIND_DAILY'");
	
	# get execution time from older Transactions table in DataIngestionInfrastructure
	#my $get_transaction = $dii->prepare("select ExecStart, FeedBuildNumber [DataIngestionInfrastructure].[dbo].[Transactions] with (NOLOCK)
	#	where FeedId = 'DS2_EQIND_DAILY'
	#	and Number = ?");
	wout(2, "getting date to start from...");
	$get_start->execute($exchcode);
	my @gstart = $get_start->fetchrow_array();
	$get_start->finish();
	my $date_begin = $date_begin_init;
	if ($gstart[0]) {	
		$gstart[0] =~ m/(\d{4})-(\d{2})-(\d{2})/;
		$date_begin = sprintf("%u%02u%02u", $1,$2,$3);
	}
	wout(2, "starting from $date_begin");
	wout(2,"retrieving transactions...");
	
	$get_trans->execute($exchcode, $date_begin);
	wlog(2, "done with query");
	
	wout(2,"examining transactions...");
	# hash for tracking transactions that for some reason do not exist in both tables
	my %errors = ();
	# hash for tracking the transaction count per date
	my %trans_info = ();
	# hash for tracking MarketInfo table results per transaction ID
	my %marketinfo = ();
	# hash for tracking infocode transactions
	my %infocodes = ();
	
	# look at each transaction 
	while (my @trans = $get_trans->fetchrow_array()) {
		my ($t_id, $marketdate, $infocode) = @trans;
		
		wlog(3, "fetched row: $t_id, $marketdate, $infocode");
		
		# skip this row if it is in the error hash
		if (exists $errors{$t_id}) {
			wlog(3, "row failed to match");
			next;	
		}
		# look at marketinfo row and get execution date + build num for ref
		if (! exists $marketinfo{$t_id}) {
			$get_marketinfo->execute($t_id);
			my @mi_row = $get_marketinfo->fetchrow_array();
			$get_marketinfo->finish();
			# insert code to find row on another table here (DS)
			if (! $mi_row[0]) {
				 wlog(3, "row failed to match: $t_id");
				 $errors{$t_id} = 1;
				 next;
			}
			# check if execution time is too far in the future, throw out
			#elsif (compedtmd($mi_row[0],$marketdate)) {
			#	wlog(3, "row failed to match");
			#	 $errors{$t_id} = 1;
			#	 next;
			#}
			
			# buildnum, filenum, filedate for future NN inputs
			wlog(4, "added new marketinfo row: $mi_row[0], $mi_row[1], $mi_row[2], $mi_row[3]");
			($marketinfo{$t_id}{'edt'}, $marketinfo{$t_id}{'bnum'}, $marketinfo{$t_id}{'fnum'}, $marketinfo{$t_id}{'fdate'}) = @mi_row;
		}
		
		$trans_info{$marketdate}{'count'}++;
		push @{$infocodes{$infocode}{$marketdate}}, $t_id;
		
		#push @{$trans_info{$marketdate}{'ids'}}, $t_id;
	}
	$get_trans->finish();

	wout(2, "examining infocode transactions...");

	# find the earliest 'complete' transaction for each infocode
	foreach my $ic (keys %infocodes) {
		foreach my $date (keys %{$infocodes{$ic}}) {
			my @edates = ();
			#wout (2, "working on $ic:$date\n");
			foreach my $t_id (@{$infocodes{$ic}{$date}}) {
				#wout(3, "pushing to array $marketinfo{$t_id}{'edt'}");
				push @edates, $marketinfo{$t_id}{'edt'};
			}
			@edates = sort @edates;
			#wout(2,"lowest exec date: $edates[0]");
			push @{$trans_info{$date}{'edts'}}, $edates[0];
		}
	} 

	# count number of infocodes for a particular date/exchange
	my $get_md_counts = $ds2->prepare("select marketdate, count(infocode) 
			from [DataStream2].[dbo].[DS2PrimQtPrc] with (NOLOCK)
			where marketdate > $date_begin
			and exchintcode = $exchcode
			group by marketdate
			order by 1 ASC");
			
	wout(2, "retrieving infocode counts...");
	$get_md_counts->execute();
	
	

	# hash for tracking the finish times per date
	my %finishtimes = ();
		
	wout(2, "calculating finish times...");
	# look at each date and calculate completion time
	while (my @md_counts = $get_md_counts->fetchrow_array()) {
		my ($date, $mdcount) = @md_counts;
		
		# skip for dates that don't exist for whatever reason
		if (! exists $trans_info{$date}) {
			wout(3, "skipping $date");
			next;
		}
		
		wlog(3,"examining date: $date");
		my @trans_times = ();
		my %buildnums = ();
		# get all execution times for that day in an array
		#foreach my $t_id (@{$trans_info{$date}{'ids'}}) {
		#	push @trans_times, $marketinfo{$t_id}{'edt'};
		#	$buildnums{$marketinfo{$t_id}{'bnum'}} = 1;
		#}
		# sort times ascending
		my @sorted = sort @{$trans_info{$date}{'edts'}};
		
		# calculate when we have received $completion% of the transactions
		# take that time as completion time for the date
		# figure out when 98% of mdcount is reached, not 98% of the total...
		@trans_times = splice(@sorted, 0, $config->calc_completion() * $mdcount);
		$finishtimes{$date}{'complete'} = $trans_times[-1];
		$finishtimes{$date}{'count'} = $mdcount;
		$finishtimes{$date}{'builds'} = scalar (keys %buildnums);
	}
	
	$get_md_counts->finish();
	
	$disfl->disconnect();
	$ds2_c->disconnect();
	$ds2->disconnect();
	$nndb->disconnect();
	
	return %finishtimes;

}



# retrieve exchanges from database if log not present
sub load_exchanges {
	
	
	if (! -e $exchange_log) {
		my $ds2_c = DBI->connect($config->ds2c_connection()) or die("Couldn't connect to DS2_change: $!\n");  
		my $ds2 = DBI->connect($config->ds2_connection()) or die("Couldn't connect to DS2: $!\n");
		
		# get a list of all the exchanges present in DS2 
		my $get_exchanges = $ds2_c->prepare("SELECT distinct ExchIntCode
  			FROM [DataStream2_Change].[arc].[DS2PrimQtPrc]
  			where MarketDate > '$date_begin_init'");
  		my $get_exchname = $ds2->prepare("SELECT [ExchName]
  			FROM [DataStream2].[dbo].[DS2Exchange] with (NOLOCK) 
  			where ExchIntCode = ?");
		wout(1,"retrieving exchange list from server...");
		$get_exchanges->execute();
		open(EXCHLOG, '>', $exchange_log);
		while (my @row = $get_exchanges->fetchrow_array()) {
			if (! $row[0]) {
				next;
			}
			$get_exchname->execute($row[0]);
			my @exchname = $get_exchname->fetchrow_array(); 
			
			#asciiify(\$row[1]);
			print EXCHLOG "$row[0],$exchname[0]\n";
			
			# check directory exists for this exchange
			#if (! -d "logs/$row[0]") {
			#	mkdir("logs/$row[0]");
			#}
		}
		$get_exchanges->finish();
		close EXCHLOG;
		$ds2_c->disconnect();
		$ds2->disconnect();
	}
	else {
		my $creation = ctime(stat($exchange_log)->mtime);
		wout(1,"found exchange log ($creation)");
	}		
}