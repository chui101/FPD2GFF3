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

# run the queue filler
push @threads,threads->new(\&queuefiller);
print "Thread 1 (queuefiller) created\n" if $verbose;

# start the worker threads
for (1 .. $maxthreads) {
	my $t = threads->new(\&worker);
	push (@threads,$t);
	print "Thread " . $t->tid() ." (worker) created\n" if $verbose;
}

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
	FPD::Config::connection("gbrowse")->use_database_handle(sub { $gbdbh = shift; });

	my $sth = $gbdbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'gff3%' LIMIT 1000;");
	$sth->execute();

	while (my $row = $sth->fetchrow_hashref()) {
		# for each feature, put it in the queue
		my %feature:shared;
		$feature{name} = $row->{gname};
		# if feature has both type and track number (?) then split it
		if ($row->{gclass} =~ /(\w+):(\d+)/) {
			$feature{type} = $1;
			$feature{track} = $2;
		# otherwise the whole thing is stored
		} else {
			$feature{type} = $row->{gclass};
			$feature{track} = "";
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
	return;
}


### worker: decode each feature into gff3
# for each feature queued, determine what type it is and run the appropriate conversion
sub worker {
	# Connect to DB using instance settings, and get our own DB handle
	my $ppdbh;
	FPD::Config::connection("pp")->use_database_handle(sub { $ppdbh = shift; });

	while (my $feature = $workqueue->dequeue()) {
		return if $feature->{endofqueue}; # end of queue, rejoin
		
		my $gff;
		### GFF3 features
		if ($feature->{'type'} eq "gff3") {
			if ($feature->{name} =~ /^\w+.(\d+)( .*)?$/) {
				# we could just use GFF3->from_db but it's not thread safe :(
				my $sth = $ppdbh->prepare("select gff3.*, geneid from gff3 left join gff3gene on gffid = gff3.id where id=?");
				$sth->execute($1);
				my $row = $sth->fetchrow_hashref();
				$gff = FPD::GFF3->from_row($row);
			} else {
				$gff = FPD::GFF3->from_db(textid => $feature->{name});
			}
		### Exonerate alignment features
		} elsif ($feature->{'type'} eq "exonerate") {
		### Interpro features
		} elsif ($feature->{'type'} eq "interpro") {
		### BLAST alignment features
		} elsif ($feature->{'type'} eq "blast") {
		### Fgenesh features
		} elsif ($feature->{'type'} eq "fgenesh") {
		}

		my $filename = $feature->{type};
		$filename .= "." . $feature->{track} if ($feature->{track});
		$filename .= ".gff3";
 
		{
			lock $filelock; # make sure no other threads are writing
			open FH, ">>$filename";
			print FH $gff->to_text();
			print threads->tid() . ": output $feature->{name} to $filename\n" if $verbose;
			close FH;
		}
		
	}
	return;
}

### 
