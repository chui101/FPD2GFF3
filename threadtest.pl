#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads::shared;
use Thread::Queue;

my $workqueue = Thread::Queue->new();

my @workers;

my $num_workers = 30;

threads->new(\&fillqueue)->join();

for (1 .. $num_workers) {
	push @workers, threads->new(\&worker);
}

$_->join() for @workers;


sub fillqueue {
	require DBI;
	DBI->import;

	my $dbh = DBI->connect("dbi:mysql:ef11gbrowse:localhost","plantproject","projectplant") or die;
	my $sth = $dbh->prepare("select gclass, gname from fgroup where gclass like 'gff3%' limit 100");
	$sth->execute();

	while (my $row = $sth->fetchrow_hashref()) {
		my %data:shared;
		$data{name} = $row->{gname};
		if ($row->{gclass} =~ /(\w+):(\d+)/) {
			$data{type} = $1;
			$data{dataset} = $2;
		} else {
			$data{type} = $row->{gclass};
			$data{dataset} = "";
		}
		$workqueue->enqueue(\%data);
	}
	return;
}


sub worker {
	require DBI;
	DBI->import;

	use lib  "/home/fpd/deployed/ef2011/CGI/tools/";
	require FPD::GFF3;

        my $dbh = DBI->connect("dbi:mysql:ef11gbrowse:localhost","plantproject","projectplant") or die;
	
	while (1) {
		my $href = $workqueue->dequeue_nb();
		print threads->tid() . ": dequeue ok\n";
		last unless $href;

		print threads->tid() . ": got " . $href->{name} . "\n";
	}
	print threads->tid() . ": thread terminating\n";
	return;
}

