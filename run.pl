#!/usr/bin/perl

use strict;
use threads;

BEGIN {
# get instance for use
}

# Include libraries from the target instance
use lib "/home/fpd/deployed/$instance/CGI/tools/";
use FPD::GFF3;
use FPD::Config;
use FPD::App::Gbrowse;

# Connect to the database using settings in the target instance configuration
my $dbh;
FPD::Config::connection()->use_database_handle(sub { $dbh = shift; });

### FOREACH track... 
# open 

# get type of track

# get features in each track

### FOREACH feature

# execute appropriate subroutine in a new thread
# each thread returns the GFF3 string which is appended to the output file for that track

