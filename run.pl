#!/usr/bin/perl

# FPD2GFF3 MAIN SCRIPT
# This script calls the appropriate helper functions in the modules.

use strict;
use warnings;
use threads;
use Thread::Queue;
use threads::shared;

BEGIN {
# get instance for use
# print usage if needed
}

# Include libraries from the target instance
use lib "/home/fpd/deployed/$instance/CGI/tools/";
use FPD::GFF3;
use FPD::Config;
use FPD::App::Gbrowse;

# Connect to the database using settings in the target instance configuration
my $gbdbh;
FPD::Config::connection("gbrowse")->use_database_handle(sub { $gbdbh = shift; });
my $ppdbh;
FPD::Config::connection("pp")->use_database_handle(sub { $ppdbh = shift; });


my $THREAD_LIMIT = 5;
# create queue of objects to be worked on
my $workqueue = Thread::Queue->new();
# an array of the threads. we use $THREAD_LIMIT threads.
my @threads;
# a kind-of semaphore to keep file writes atomic
my $filelock : shared;

### Get the features to process
{ 
	# get the features from the gbrowse database
	my $sth = $gbdbh->prepare("SELECT gclass, gname FROM fgroup LIMIT 100;");
	$sth->execute();

	while (my $row = $sth->fetchrow_hashref()) {
		# for each feature, put it in the queue
		my $feature = {};
		$feature->{name} = $row->{gname};
		# if feature has both type and track number (?) then split it
		if ($row->{gclass} ~= /(\w+):(\d+)/) {
			$feature->{type} = $1;
			$feature->{track} = $2;
		# otherwise the whole thing is stored
		} else {
			$feature->{type} = $row->{gclass};
		}

		# enqueue the feature
		$workqueue->enqueue($feature);
	}
}

# start the worker threads
push (@threads,threads->new(\&worker)) for 1..$THREAD_LIMIT;

# now we twiddle thumbs and wait for threads to finish working
$_->join() for @threads;

sub worker {
	while (my $feature = $workqueue->dequeue_nb()) {
		return if ($feature == undef); # end of queue, rejoin
		# query data for the feature
		if ($feature->{'type'} eq "fgenesh") {
			# do the query for fgenesh
		}

		# put it through FPD::GFF3
		my $gff = FPD::GFF3->new();
		$gff->{refseq} = undef;
		$gff->{start} = undef;
		$gff->{end} = undef;
		$gff->{source} = undef;
		$gff->{method} = undef;
		$gff->{strand} = undef;
		$gff->{score} = undef;
		$gff->{refseq} = undef;
		$gff->{refseq} = undef;

		$filename = $feature->{type} . $feature->{track} . ".gff3";
 
		{
			lock $filelock; # make sure no other threads are writing
			open FH, ">>$filename";
			# print FH $gff->to_text();
			print threads->tid() . ": output $feature->{name} to $filename\n";
			close FH;
		}
		
	}
	return;
}
