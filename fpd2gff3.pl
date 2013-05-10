#!/usr/bin/perl

# FPD2GFF3 MAIN SCRIPT
# This script calls the appropriate helper functions in the modules.

use threads;
use threads::shared;
use strict;
use warnings;
use Thread::Queue;
require "GFF3Object.pm";


use Getopt::Long qw(:config gnu_getopt);

our $maxthreads : shared;
our $verbose : shared;
our $instance : shared;

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
# Read configuration in FPD/Config without 'use' (so we remain thread-safe)

my $gbdb:shared;
my $fpdb:shared;
my $db_host:shared;

open (FH, "<", "/home/fpd/deployed/$instance/CGI/tools/FPD/Config.pm") or die "Cannot open configuration for instance $instance!";
while (<FH>) {
	if ($_ =~ /my \$PP_DB = \"(.+)\"/) {$fpdb = $1;}
	if ($_ =~ /my \$GB_DB = \"(.+)\"/) {$gbdb = $1;}
	if ($_ =~ /my \$GB_HOST = \"(.+)\"/) {$db_host = $1;}
}
# assume we're running on csbio-l
$db_host = "localhost";
$verbose and print "using databases $fpdb and $gbdb on $db_host\n";
close FH;

### Set up the global variables
# queue of objects to be worked on
my $workqueue = Thread::Queue->new();
# an array of the threads. we use $maxthreads threads.
my @threads;
# kind-of mutex to keep file writes atomic
my $filelock : shared;
my %seen_query : shared;

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
	require DBI;
	DBI->import;

	my $dbh = DBI->connect("dbi:mysql:$gbdb:$db_host","plantproject","projectplant") or die;

	# query for gff3, add to queue
	my $sth = $dbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'gff3%' LIMIT 1000;");
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

	# query for fgenesh, add to queue
	my $sth = $dbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'fgene%' LIMIT 1000;");
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
	$sth = $dbh->prepare("SELECT gclass, gname FROM fgroup WHERE gclass LIKE 'exonerate%' LIMIT 1000;");
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
	require DBI;
	DBI->import;
        my $ppdbh = DBI->connect("dbi:mysql:$fpdb:$db_host","plantproject","projectplant") or die;

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
		my $filename = $instance . "_" . $feature->{type};
		$filename .= "." . $feature->{dataset} if ($feature->{dataset});
		$filename .= ".gff3";

		# lock file handle for writing, output gff line to file
		{
			lock $filelock; # make sure no other threads are writing
			unless (-e $filename) {
				open FH, ">$filename";
				print FH "##gff-version 3\n";
				close FH;
			}
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
	my $gff3 = GFF3Object->new();
	if ($feature->{name} =~ /^\w+\.(\d+)( .*)?$/) {
		my $sth = $dbh->prepare("select gff3.*, geneid from gff3 left join gff3gene on gffid = gff3.id where id=?");
		$sth->execute($1);
		my $row = $sth->fetchrow_hashref();
		$gff3 = GFF3Object->from_hash($row);
	} else {
		my $sth = $dbh->prepare("select * from gff3 where text_id=?");
		$sth->execute($feature->{name});
		my $row = $sth->fetchrow_hashref();
		$gff3 = GFF3Object->from_hash($row);
	}
	return $gff3;
}

### Exonerate alignments
sub exonerate_to_gff3 {
	my ($feature, $dbh) = @_;
	my ($vulgar, $local_start, $realquery, $model);
	if ($feature->{name} =~ /^exonerate\.(\d+)/) {
		my $dbid = $1;
		my $sth = $dbh->prepare("select exonerate.target as target,exonerate.vulgar as vulgar, exonerate.model as model, exongene.geneid as geneid, exonerate.query as query from exonerate join exongene on exonerate.entryid = exongene.entryid where exonerate.entryid=?");
		$sth->execute($dbid);
		my $row = $sth->fetchrow_hashref();
		($vulgar, $local_start, $realquery, $model) = ($row->{vulgar},1,$row->{query},$row->{model});
	} else { 
		warn "bad feature: " . $feature->{name} . "\n";
		return;
	}
	my (
		$query, $qst, $qend, $qstrand,
		$target, $tst, $tend, $tstrand,
		$score, $alignment
	) = ($vulgar =~ /
		^(\S+)\s+(\d+)\s+(\d+)\s+([+-.])\s+
		 (\S+)\s+(\d+)\s+(\d+)\s+([+-])\s+
		 (\d+)\s+(.*)
		/x
	);
	$query = $realquery if defined $realquery;
	if (defined $local_start) {
		$tst += ($local_start - 1);
		$tend += ($local_start - 1);
	}
	# print STDERR ">>> q $query:$qst..$qend  t $target:$tst..$tend  s $score\n";
	my $gid = $query;
	$gid =~ s/ /_/g;
	$query =~ /GI=([^|]+)/ and $gid = $1;
	# $query =~ /LOCUS=([^|]+)/ and $gid = $1;  # Locus can be too long
	$query =~ /ACCESSION=([^|]+)/ and $gid = $1;
	$query =~ /PROTEIN_ID=([^|]+)/ and $gid = $1;
	$query =~ /^([A-Z]+\d+(?:\.\d+)?)\s/ and $gid = $1; # Starts with an ID
	++$seen_query{$gid};
	my $geneid = "$gid:match:$seen_query{$gid}";
	defined $alignment or die "bad line '$vulgar'";
	my $qsign = ($qstrand eq '-') ? -1 : 1;
	my $tsign = ($tstrand eq '-') ? -1 : 1;

	my $currpos = $tst;
	# the smaller end of the range is 'off by one', because exonerate
	# reports position between bases (0 means 'just before the first
	# base', 1 'just after the first base').
	++$currpos if $tstrand eq '+';


	my $genegff = new GFF3Object;
	$genegff->{refseq} = $target;
	$genegff->{start} = $tsign < 0 ? $tend+1 : $tst+1;
	$genegff->{end} = $tsign < 0 ? $tst : $tend;
	$genegff->{source} = "$model";
	$genegff->{method} = ($model =~ /protein/) ? "protein_match" : "expressed_sequence_match";
	$genegff->{strand} = $tstrand;
	$genegff->{score} = $score;

	my $encquery = $query;
	$encquery =~ s/ /%20/g;	

	$genegff->set_attr(Target => sprintf(
			"%s %d %d %s", $encquery,
			$qsign < 0 ? $qend+1 : $qst + 1,
			$qsign < 0 ? $qst : $qend,
			$qstrand
		)
	);
	
	$genegff->{end} = $tsign < 0 ? $tst : $tend;

	$genegff->set_attr(Name => $query);
	$genegff->set_attr(ID => $geneid);
	$genegff->set_attr(vulgar => $alignment);

	my $currgff = undef;
	my $ino = 0;
	my $eno = 0;

	while ($alignment =~ s/^\s*(\S+) (\d+) (\d+)//) {
		my ($type, $qlen, $tlen) = ($1,$2,$3);

		if ($type =~ /[IF53N]/) {
			# Intron, frame shift, or non-equivalenced region
			if (defined $currgff and $currgff->{method} ne "intron") {
				if($tsign < 0) {
					($currgff->{end}, $currgff->{start}) = ($currgff->{start}, $currgff->{end});
				}
				#print $currgff->to_text();
				$currgff = undef;
			}
		} elsif ($type =~ /[MCGS]/) {
			# Part of an exon
			$currgff = new GFF3Object;
			$currgff->{refseq} = $target;
			$currgff->{start} = $currpos;
			$currgff->{source} = $model;
			$currgff->{method} = "match_part";
			$currgff->{strand} = $genegff->{strand};
			$currgff->set_attr(ID => "$geneid:exon:" . ++$eno);
			#$currgff->set_attr(Parent => $geneid);
			$currgff->{end} = $currpos + $tsign*($tlen-1);
			# add the created feature to the list of children
			$genegff->add_child($currgff);
		} else {
			warn "Unknown type '$type' in $query:$target";
		}

		$currpos += $tsign * $tlen;
	}
	if (defined $currgff) {
		if($tsign < 0) {
			($currgff->{end}, $currgff->{start}) = ($currgff->{start}, $currgff->{end});
		}
		#print $currgff->to_text();
	} else {
		warn "3' splice junction without following exon\nin $query:$target\n";
	}
	return $genegff;
}

### Interpro features
# interpro is a bit different. each feature can have multiple database entries (for different parts of the feature). remember to loop through all of them...
sub interpro_to_gff3 {
	my ($feature,$dbh) = @_;
	my $gff3 = GFF3Object->new();
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
	my ($feature,$dbh) = @_;
	my $sth;

	# extract gene id and run id from feature name
	my ($geneid, $runid) = ($feature->{name} =~ /^A\.(\d+)\.(\d+)/);
	
	# get contig ID for run
	$sth = $dbh->prepare("select c.name,fgenesh.score from c join fgenesh on fgenesh.contigid = c.id where runid = ?");
	$sth->execute($runid);
	my ($contig,$score) = $sth->fetchrow_array();
	$contig = sprintf("contig%05d",$contig); #zero pad
	$score = 0 unless $score;

	# get bounds on gene and mrna, also get strand
	$sth = $dbh->prepare("select least(min(start),min(end)),greatest(max(start),max(end)),strain from fgenedata where runid = ? and genenumber = ?");
	$sth->execute($runid,$geneid);
	my ($genestart, $geneend, $strand) = $sth->fetchrow_array();

	$sth = $dbh->prepare("select least(min(start),min(end)),greatest(max(start),max(end)) from fgenedata where runid = ? and genenumber = ? and entrypos > 0");
	$sth->execute($runid,$geneid);
	my ($mrnastart, $mrnaend) = $sth->fetchrow_array();

	# set up new GFF3 Objects for gene and populate info
	my $gff = new GFF3Object;
	$gff->set_attr(ID => $feature->{name});
	$gff->set_attr(name => $feature->{name});
	$gff->set_attr(refseq => $contig);
	$gff->set_attr(source => "fgenesh");
	$gff->set_attr(method => "primary_transcript");
	$gff->set_attr(start => $genestart);
	$gff->set_attr(end => $geneend);
	$gff->set_attr(score => $score);
	$gff->set_attr(strand => $strand);
	
	# set up GFF3Object for TSS and populate info if it exists
	$sth = $dbh->prepare("select score,start from fgenedata where runid = ? and genenumber = ? and feature = 'TSS'");
	$sth->execute($runid,$geneid);
	if (my @row = $sth->fetchrow_array()) {
		my $tss = new GFF3Object;
		$tss->set_attr(ID => $feature->{name}.":TSS");
		$tss->set_attr(name => $feature->{name}.":TSS");
		$tss->set_attr(refseq => $contig);
		$tss->set_attr(source => "fgenesh");
		$tss->set_attr(method => "TSS");
		$tss->set_attr(start => $row[1]);
		$tss->set_attr(end => $row[1]);
		$tss->set_attr(score => $row[0]);
		$tss->set_attr(strand => $strand);
		$gff->add_child($tss);
	}

	# set up GFF3Object for mRNA part
	my $mrna = new GFF3Object;
	$mrna->set_attr(ID => $feature->{name}.":primary_transcript_region");
	$mrna->set_attr(name => $feature->{name}.":primary_transcript_region");
	$mrna->set_attr(refseq => $contig);
	$mrna->set_attr(source => "fgenesh");
	$mrna->set_attr(method => "primary_transcript_region");
	$mrna->set_attr(start => $genestart);
	$mrna->set_attr(end => $geneend);
	$mrna->set_attr(score => $score);
	$mrna->set_attr(strand => $strand);
	$gff->add_child($mrna);

	# get and loop through CDSes
	$sth = $dbh->prepare("select start,end,score,feature,entrypos from fgenedata where runid = ? and genenumber = ? and entrypos > 0");
	$sth->execute($runid,$geneid);
	my $last_len = 0;
	my $last_phase = 0;
	while (my @row = $sth->fetchrow_array()) {
		my $cds = new GFF3Object;
		$cds->set_attr(ID => $feature->{name}.":CDS");
		$cds->set_attr(name => $feature->{name}.":CDS");
		$cds->set_attr(refseq => $contig);
		$cds->set_attr(source => "fgenesh");
		$cds->set_attr(method => "CDS");
		$cds->set_attr(start => $row[0]);
		$cds->set_attr(end => $row[1]);
		$cds->set_attr(score => $row[2]?$row[2]:0);
		$cds->set_attr(strand => $strand);
		my $phase = ($last_phase - $last_len) % 3;
		$last_len = $row[1] - $row[0] + 1;
		$last_phase = $phase;
		$cds->set_attr(phase => $phase);
		$mrna->add_child($cds);
	}
	
	# set up TES	
	$sth = $dbh->prepare("select score,start from fgenedata where runid = ? and genenumber = ? and feature = 'PolA'");
	$sth->execute($runid,$geneid);
	if (my @row = $sth->fetchrow_array()) {
		my $tes = new GFF3Object;
		$tes->set_attr(ID => $feature->{name}.":transcription_end_site");
		$tes->set_attr(name => $feature->{name}.":transcription_end_site");
		$tes->set_attr(refseq => $contig);
		$tes->set_attr(source => "fgenesh");
		$tes->set_attr(method => "transcription_end_site");
		$tes->set_attr(start => $row[1]);
		$tes->set_attr(end => $row[1]);
		$tes->set_attr(score => $row[0]);
		$tes->set_attr(strand => $strand);
		$gff->add_child($tes);
	}

	return $gff;
}
