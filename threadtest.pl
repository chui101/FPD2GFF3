#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads::shared;
use Thread::Queue;

my $workqueue = Thread::Queue->new();

my @workers;

my $num_workers = 30;

fillqueue();

for (1 .. $num_workers) {
	push @workers, threads->new(\&worker);
}

$_->join() for @workers;


sub fillqueue {
	for (1 .. 1000) {
		my %data:shared;
		$data{foo} = 'bar';
		$data{hello} = 'world';
		$workqueue->enqueue(\%data);
	}
	return;
}


sub worker {
	require DBI;
	DBI->import;
	while (1) {
		my $href = $workqueue->dequeue_nb();
		print threads->tid() . ": dequeue ok\n";
		last unless $href;
		for (1 .. 10) {
			$href->{foo} = $href->{hello};
		}
	}
	return;
}


