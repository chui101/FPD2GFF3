#!/usr/bin/perl

# FPD2GFF3 MAIN SCRIPT
# This script calls the appropriate helper functions in the modules.

use forks;
use forks::shared;
use strict;
use warnings;
use Thread::Queue;

use Getopt::Long qw(:config gnu_getopt);

our $maxthreads : shared;
our ($verbose, $instance);

BEGIN {
# get instance for use
	sub usage(;$) {
		my $status = shift || 0;
		my $out = $status ? *STDERR : *STDOUT;
		print $out <<EOF;
Usage: $0 [<options>] <db-instance>

Exports all features in specified db-instance to GFF3 files. Each track and
feature type will be put into a separate GFF3 file.

Options:
  -t | --threads n       run with n worker threads
  -h | --help            show this help
  -v | --verbose         show all output
EOF
		exit $status;
	}

	my $help;
	GetOptions(
			't|threads=i' =>\$maxthreads,
			'h|help' => \$help,
			'v|verbose' => \$verbose,
		  ) or usage(1);

	$help and usage(0);
	@ARGV == 1 or usage(2);
	$maxthreads or $maxthreads = 4;
	$instance = shift;
}
### Include libraries from the target instance
use lib "/home/fpd/deployed/$instance/CGI/tools/";
use FPD::Config;
use FPD::GFF3;

### Set up the global variables
# queue of objects to be worked on
my $workqueue = Thread::Queue->new();
# an array of the threads. we use $maxthreads threads.
my @threads;
# kind-of mutex to keep file writes atomic
my $filelock : shared;

# start the worker threads
for (1 .. $maxthreads) {
	my $t = threads->new(\&worker);
	push (@threads,$t);
	print "Thread " . $t->tid() ." (worker) created\n" if $verbose;
}

# run the queue filler
my $qf = threads->new(\&queuefiller);
push @threads,$qf;
print "Thread ".$qf->tid()." (queuefiller) created\n" if $verbose;

# now we twiddle our thumbs and wait for threads to finish working
$_->join() for @threads;

### done!
exit(0);

##### subs below this point #####

### queuefiller: Get the features to process
# get the features from the gbrowse database and give the workers tasks
sub queuefiller {
	# Connect to the database using settings in the target instance configuration
	my $gbdbh;
	my $sth;
	FPD::Config::connection("gbrowse")->use_database_handle(sub { $gbdbh = shift; });

	# query for gff3, add to queue
	$sth = $gbdbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'gff3%' LIMIT 1000;");
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref()) {
		# for each feature, put it in the queue
		my %feature:shared;
		$feature{name} = $row->{gname};
		# if feature has both type and dataset then split it
		if ($row->{gclass} =~ /^(\w+)\:(\d+)$/) {
			$feature{type} = $1;
			$feature{dataset} = $2;
		# otherwise the whole thing is stored
		} else {
			$feature{type} = $row->{gclass};
			$feature{dataset} = "";
		}

		# enqueue the feature
		$workqueue->enqueue(\%feature);
	}

	# query for exonerate, add to queue
	$sth = $gbdbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'exonerate%' LIMIT 1000;");
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref()) {
		# for each feature, put it in the queue
		my %feature:shared;
		$feature{name} = $row->{gname};
		# if feature has both type and dataset then split it
		if ($row->{gclass} =~ /^(\w+)\:(\d+)$/) {
			$feature{type} = $1;
			$feature{dataset} = $2;
		# otherwise the whole thing is stored
		} else {
			$feature{type} = $row->{gclass};
			$feature{dataset} = "";
		}

		# enqueue the feature
		$workqueue->enqueue(\%feature);
	}

	# enqueue $maxthreads number of EOQ signals
	for (1 .. $maxthreads) {
		my %f:shared;
		$f{endofqueue} = 1;
		$workqueue->enqueue(\%f);
	}

	# done filling, return
	print "Thread " . threads->tid() . " (queuefiller) done.\n" if $verbose;
	return;
}


### worker: decode each feature into gff3
# for each feature queued, determine what type it is and run the appropriate conversion
sub worker {
	# Connect to DB using instance settings, and get our own DB handle
	my $ppdbh;
	FPD::Config::connection("pp")->use_database_handle(sub { $ppdbh = shift; });

	while (my $feature = $workqueue->dequeue()) {
		last if $feature->{endofqueue}; # end of queue, exit the loop
		
		my $gff;

		# get the GFF3 object based on what kind of feature we picked off the queue
		# ship work off to a sub so we don't clutter the worker function
		if ($feature->{'type'} eq "gff3") {
			$gff = gff3_to_gff3($feature, $ppdbh);
		} elsif ($feature->{'type'} eq "exonerate") {
			$gff = exonerate_to_gff3($feature, $ppdbh);
		} elsif ($feature->{'type'} eq "interpro") {
			$gff = interpro_to_gff3($feature, $ppdbh);
		} elsif ($feature->{'type'} eq "blast") {
			$gff = blast_to_gff3($feature,$ppdbh);
		} elsif ($feature->{'type'} eq "fgenesh") {
			$gff = fgenesh_to_gff3($feature,$ppdbh);
		}

		# create the appropriate file name to write to
		my $filename = $feature->{type};
		$filename .= "." . $feature->{dataset} if ($feature->{dataset});
		$filename .= ".gff3";

		# lock file handle for writing, output gff line to file
		{
			lock $filelock; # make sure no other threads are writing
			open FH, ">>$filename";
			print FH $gff->to_text();
			print threads->tid() . ": output $feature->{name} to $filename\n" if $verbose;
			close FH;
		}
		
	}
	print "Worker " . threads->tid() . " done\n" if $verbose;
	return;
}

###

### GFF3 features
sub gff3_to_gff3 {
	my ($feature, $dbh) = @_;
	my $gff3 = FPD::GFF3->new();
	if ($feature->{name} =~ /^\w+\.(\d+)( .*)?$/) {
		my $sth = $dbh->prepare("select gff3.*, geneid from gff3 left join gff3gene on gffid = gff3.id where id=?");
		$sth->execute($1);
		my $row = $sth->fetchrow_hashref();
		$gff3 = FPD::GFF3->from_row($row);
	} else {
		my $sth = $dbh->prepare("select * from gff3 where text_id=?");
		$sth->execute($feature->{name});
		my $row = $sth->fetchrow_hashref();
		$gff3 = FPD::GFF3->from_row($row);
	}
	return $gff3;
}

### Exonerate alignments
sub exonerate_to_gff3 {
	my ($feature, $dbh) = @_;
	my $gff3 = FPD::GFF3->new();
	if ($feature->{name} =~ /^exonerate\.(\d+)/) {
		my $dbid = $1;
		my $sth = $dbh->prepare("select exonerate.target as target,exonerate.vulgar as vulgar, exonerate.model as model, exongene.geneid as geneid from exonerate join exongene on exonerate.entryid = exongene.entryid where exonerate.entryid=?");
		$sth->execute($dbid);
		my $row = $sth->fetchrow_hashref();
		my %gff3hash;

		# vulgar format: (query id) (query start) (query end) (query strand) (target id) (target start) (target end) (target strand) (score) (CIGAR gap string)
		# split it on the spaces so we can just shift it away
		my @vulgar = split ' ',$row->{vulgar};
			
		# map as many of the query/vulgar attributes to gff3 attributes as we can...
		$gff3hash{id} = "exonerate.$dbid";
		$gff3hash{phase} = '.';
		$gff3hash{source} = 'exonerate';
		$gff3hash{method} = $row->{model};
		$gff3hash{text_id} = "exonerate.$dbid";
		$gff3hash{name} = shift @vulgar;
		my $qstart = shift @vulgar; # querystart
		my $qend = shift @vulgar; # queryend
		my $qstrand = shift @vulgar; # querystrand
		shift @vulgar; # target-nonformatted
		$gff3hash{refseq} = $row->{target};
		$gff3hash{start} = shift @vulgar; 
		$gff3hash{end} = shift @vulgar;
		$gff3hash{strand} = shift @vulgar;
		$gff3hash{score} = shift @vulgar;
		$gff3hash{geneid} = $row->{geneid};
		# TODO: assembly/dataset/importid?

		# convert CIGAR gap string to GFF3 CIGAR gapping
		my $gap;
		while (scalar @vulgar) {
			my $gtype = shift @vulgar;
			if ($gtype eq '5') {
				# this is an intron 5' splice site. next two triplets are going to be the intron and 3' splice site
				shift @vulgar; # burn a query strand entry
				my $fivesplice = shift @vulgar;
				shift @vulgar; # should be an I for intron... not interested
				shift @vulgar; # burn a query strand entry
				my $gapsize = shift @vulgar;
				shift @vulgar; # should be a 3... not interested
				shift @vulgar; # burn a query strand entry
				my $threesplice = shift @vulgar;
				# add the splice sites to total gap size
				$gapsize = $gapsize + $fivesplice + $threesplice;
				# exon is always going to be a gap in the exonerate query
				$gap .= "D$gapsize ";
			} elsif ($gtype eq 'G') {
				# this is a gap I or D, representing a gap in the I=GFF3 reference/exonerate target or the D=GFF3 target/exonerate query
				my $rgapsize = shift @vulgar;
				my $tgapsize = shift @vulgar;
				my $gaptype = ($rgapsize?'I':'D');
				my $gapsize = ($rgapsize?$rgapsize:$tgapsize);
				$gap .= "$gaptype$gapsize ";
			} elsif ($gtype eq "I") {
				# this is a , representing gap in GFF3 target/exonerate query
				shift @vulgar;
				my $gapsize = shift @vulgar;
				$gap .= "D$gapsize ";
			} elsif ($gtype eq "M") {
				# a match is a match is a match
				shift @vulgar;
				my $gapsize = shift @vulgar;
				$gap .= "M$gapsize ";
			}
		}

		chop $gap; #remove the last space from the gapping we just made

		# create the gff3 object
		$gff3 = FPD::GFF3->from_row(\%gff3hash);
		# set target and gap attributes in the gff3 object
		$gff3->set_attr("Target",$gff3hash{name} . " $qstart $qend $qstrand"); #gff3 target attribute -> exonerate query
		$gff3->set_attr("Gap",$gap);
	}
	#TODO: what if it doesn't match the pattern?
	return $gff3;
}

### Interpro features
# interpro is a bit different. each feature can have multiple database entries (for different parts of the feature). remember to loop through all of them...
sub interpro_to_gff3 {
	my ($feature,$dbh) = @_;
	my $gff3 = FPD::GFF3->new();
	if ($feature->{name} =~ /^ipr\.(\d+)\.(\d+)/) {
		my $runid = $1;
		my $resultid = $2;
		my $sth = $dbh->prepare("select ");
	}
# select m.id, m.resultid, r.runid, m.ord, m.name, m.dbname, m.dbid, l.start, l.end from ip_matches m inner join ip_results as r on r.id = m.resultid inner join ip_locations as l on l.matchid = m.id where r.runid=?;
# convert to contig coordinates
# need the referenced ORF... ip_link?
	
}


### BLAST alignments
sub blast_to_gff3 {
	my ($feature, $dbh) = @_;
	my $gff3;
}

### FGenesh features
sub fgenesh_to_gff3 {
}

