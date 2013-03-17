# FPD2GFF3
A PERL script collection to convert and export FPD database information to GFF3 format for use in GBrowse 2.

## Features
- Modular
- Multi-process, scalable across many CPUs

## Requirements
- PERL 5.8+
- Perl modules:
	- FPD
	- Thread::Queue
	- forks

## Usage
	run.pl [options] (instance)
	
	(instance) is the FPD/GBrowse instance to use. The name corresponds to the directory in /home/fpd/deployed
	
	options:
	-t,--threads	number of worker &quot;threads&quot; to use
	-h,--help	show help message
	-v,--verbose	show verbose output

## What it does
For each instance, the script dumps the list of features from the gbrowse/fgroup database table and populates a work queue.
The specified number of workers (default 4) are then spawned to eat through the work queue, taking each feature and placing
it in a GFF3 format file corresponding to the feature type and dataset number.
