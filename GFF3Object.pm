#! /usr/bin/perl -w

package GFF3Object;
use strict;
use Carp;
use URI::Escape;

sub field_escape($;$) {
	my $str = shift;
	my $esc_ws = shift;
	return uri_escape($str, "\\\"\x00-\x1f;=%&," . ($esc_ws ? " " : ""))
}

# coalesce copied from FPD::Common
# Return the first argument that is defined and not the empty string.  Like
#  $arg1 || $arg2 || ... || $argn,  except "0" is treated as "true".
sub coalesce(@) {
	for my $arg (@_) {
		defined($arg) and ($arg ne "") and return $arg;
	}
	return undef;
}


# Private functions
sub attr_string($) {
	my $self = shift;
	my @attrstrs = ();

	if (defined $self->{text_id}) {
		push @attrstrs, "ID=" . field_escape($self->{text_id});
	}
	if (defined $self->{name}) {
		push @attrstrs, "Name=" . field_escape($self->{name});
	}
	# 'Parent' is stored both here and in the attrs; we'll use the latter.

	for my $key (sort keys %{$self->{attrs}}) {
		my $value = $self->{attrs}{$key};
		next unless defined $value;
		if (ref($value) eq "ARRAY") {
			push @attrstrs, field_escape($key) . "=" . join(",",
				map { field_escape $_ } @$value
			);
		} else {
			push @attrstrs, field_escape($key) . "="
			   . field_escape($value);
		}
	}

	return join(";", @attrstrs) . ";";
}

# Public methods
sub get_attr($$) {
	my ($self, $key) = @_;
	if ($key =~ /^ID$/i || $key eq "text_id") {
		return $self->{text_id};
	} elsif ($key =~ /^[nN]ame$/) {
		return $self->{name};
	} elsif (exists $self->{$key}) {
		return $self->{$key};
	} elsif (exists $self->{attrs}{$key}) {
		my $val = $self->{attrs}{$key};
		return @$val if wantarray;
		return $val->[0];
	} else {
		return () if wantarray;
		return undef;
	}

}

sub set_attr($$@) {
	my ($self, $key, @values) = @_;
	my $valcnt = scalar @values;

	$self->remove_attr($key) unless $valcnt;
	# croak "GFF3::set_attr($key): always need at least one value" unless $valcnt;

	if ($key =~ /^ID$/i || $key eq "text_id") {
		$valcnt == 1 or
			croak "GFF3::set_attr: ID wants one value, not $valcnt";
		$self->{text_id} = $values[0];
	} elsif ($key =~ /^[nN]ame$/) {
		# If name contained commas, we have an improperly escaped GFF;
		# rejoin the parts
		$valcnt == 1 or $values[0] = join ",", @values;
		$self->{name} = $values[0];
	} elsif ($key =~ /^[pP]arent$/) {
		$self->{parent} = $values[0];
		$self->{attrs}{Parent} = [@values];
	} elsif (grep { $_ eq $key } $self->base_attrs()) {
		$valcnt == 1 or
			croak "GFF3::set_attr: $key wants one value, not $valcnt";
		$self->{$key} = $values[0];
	} else {
		$self->{attrs}{$key} = [@values];
	}
	return $self;
}

sub append_attr($$@) {
	my ($self, $key, @values) = @_;
	my $valcnt = scalar @values;

	# No values; do nothing.
	return $self unless $valcnt;

	if ($key =~ /^ID$/i || $key eq "text_id") {
		$valcnt == 1 or
			croak "GFF3::append_attr: ID wants one value, not $valcnt";
		defined $self->{text_id} and
			croak "GFF3::append_attr: already have an ID";
		$self->{text_id} = $values[0];
	} elsif ($key =~ /^[nN]ame$/) {
		defined $self->{name} and
			croak "GFF3::append_attr: already have a name";
		# If name contained commas, we have an improperly escaped GFF;
		# rejoin the parts
		$valcnt == 1 or $values[0] = join ",", @values;
		$self->{name} = $values[0];
	} elsif ($key =~ /^[pP]arent$/) {
		defined $self->{parent} or $self->{parent} = $values[0];
		push @{$self->{attrs}{Parent}}, @values;
	} elsif (grep { $_ eq $key } $self->base_attrs()) {
		$valcnt == 1 or
			croak "GFF3::append_attr: $key wants one value, not $valcnt";
		defined $self->{$key} and
			croak "GFF3::append_attr: already have a $key";
		$self->{$key} = $values[0];
	} else {
		push @{$self->{attrs}{$key}}, @values;
	}
	return $self;
}

sub prepend_attr($$@) {
	my ($self, $key, @values) = @_;
	my $valcnt = scalar @values;

	# No values; do nothing.
	return $self unless $valcnt;

	if ($key =~ /^ID$/i || $key eq "text_id") {
		$valcnt == 1 or
			croak "GFF3::prepend_attr: ID wants one value, not $valcnt";
		defined $self->{text_id} and
			croak "GFF3::prepend_attr: already have an ID";
		$self->{text_id} = $values[0];
	} elsif ($key =~ /^[nN]ame$/) {
		defined $self->{name} and
			croak "GFF3::prepend_attr: already have a name";
		# If name contained commas, we have an improperly escaped GFF;
		# rejoin the parts
		$valcnt == 1 or $values[0] = join ",", @values;
		$self->{name} = $values[0];
	} elsif ($key =~ /^[pP]arent$/) {
		defined $self->{parent} or $self->{parent} = $values[0];
		unshift @{$self->{attrs}{Parent}}, @values;
	} elsif (grep { $_ eq $key } $self->base_attrs()) {
		$valcnt == 1 or
			croak "GFF3::prepend_attr: $key wants one value, not $valcnt";
		defined $self->{$key} and
			croak "GFF3::prepend_attr: already have a $key";
		$self->{$key} = $values[0];
	} else {
		unshift @{$self->{attrs}{$key}}, @values;
	}
	return $self;
}

sub remove_attr {
	my $self = shift;
	my $key = shift;

	if ($key =~ /^ID$/i || $key eq "text_id") {
		delete $self->{text_id};
	} elsif ($key =~ /^[nN]ame$/) {
		delete $self->{name};
	} elsif ($key =~ /^[pP]arent$/) {
		delete $self->{parent};
		delete $self->{attrs}{$key};
	} elsif (grep { $_ eq $key } $self->base_attrs()) {
		delete $self->{$key};
	} else {
		delete $self->{attrs}{$key};
	}
	return $self;
}

sub base_attrs {
	my $proto = shift;
	my $class = ref($proto) || $proto;

	return qw(text_id name refseq source method start end score strand phase);
}

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self = {
		dbid => undef,
		importid => undef,
		refseq => undef,
		source => undef,
		method => undef,
		start => undef,
		end => undef,
		score => undef,
		strand => undef,
		phase => undef,
		text_id => undef,
		name => undef,
		parent => undef,
		attrs => {},
		children => [],
	};
	
	bless ($self,$class);
	return $self;
}

sub clone($) {
	my $self = shift;
	my $clone = $self->new();

	for my $key ($self->base_attrs(), keys %{$self->{attrs}}) {
		$clone->set_attr($key, $self->get_attr($key));
	}

	return $clone;
}

sub start($) {
	my $self = shift;
	return $self->{start};
}

sub end($) {
	my $self = shift;
	return $self->{end};
}

sub len($) {
	my $self = shift;
	return $self->{end} - $self->{start} + 1;
}

sub refseq($) {
	my $self = shift;
	return $self->{refseq};
}

sub source($) {
	my $self = shift;
	return $self->{source};
}

sub method($) {
	my $self = shift;
	return $self->{method};
}

sub name($) {
	my $self = shift;
	return $self->{name};
}

sub id($) {
	my $self = shift;
	return $self->{text_id};
}

sub strand($) {
	my $self = shift;
	return $self->{strand};
}

sub coords($) {
	my $self = shift;

	if(wantarray) {
		return ($self->refseq, $self->start, $self->end);
	} else {
		return sprintf "%s:%d..%d", $self->coords;
	}
}

sub ref_coords() {
	my $self = shift;
	return convert_contig_region_to_reference_region($self->coords);
}

sub from_text($$) {
	my $proto = shift;
	my $line = shift;
	my $self = new $proto;

	chomp $line;
	my $attrs;

	($self->{refseq}, $self->{source}, $self->{method}, $self->{start},
		$self->{end}, $self->{score}, $self->{strand},
		$self->{phase}, $attrs) =  split /\t/, $line, 9;

	croak "GFF3::from_text: bad entry: $line" unless defined $self->{phase};
	for my $key (qw(refseq source method start end score strand phase)) {
		$self->{$key} = uri_unescape($self->{$key});
	}
 	
 	if (defined $attrs) {
		for my $attr (split /;/, $attrs) {
			my ($key, $value) = split /=/, $attr, 2;
			my @values = split /,/, $value;

			$self->set_attr(uri_unescape($key),
				map { uri_unescape $_ } @values
			);
		}
	}
	return $self;
}

# sub from_file {
# 	my $proto = shift;
# 	my $file = shift;
# 	my $filter = shift;
# 
# 	$filter = sub { return @_; } unless defined $filter;
# 
# 	my $fh;
# 	my $needs_close = 0;
# 	if (!defined $file) {
# 		$fh = \*STDIN;
# 	} elsif (ref $file) {
# 		$fh = $file;
# 	} else {
# 		$fh = new IO::File "$file", "r";
# 		$needs_close = 1;
# 	}
# 
# 	my @toplevel;
# 	eval {
# 		my %by_id;
# 		while(<$fh>) {
# 			chomp;
# 
# 			# Remove DOS line endings
# 			s/\r$//;
# 
# 			# Avoid the FASTA portion of hybrid files
# 			last if /^##FASTA/;
# 
# 			# Skip comments and blank lines
# 			next if /^#/ or /^\s*$/;
# 
# 			my $gff = GFF3Object->from_text($_);
# 
# 			# Filter the gff, skipping it if the filter returns undef
# 			next unless $filter->($gff);
# 
# 			my $id = $gff->get_attr('ID');
# 			$by_id{$id} = $gff;
# 
# 			# TODO: handle children that precede their parents
# 			my @parents = $gff->get_attr('Parent');
# 			for my $pid (@parents) {
# 				push @{$by_id{$pid}{children}}, $gff;
# 			}
# 			if (@parents == 0) {
# 				push @toplevel, $gff;
# 			}
# 		}
# 	};
# 	close $fh if $needs_close;
# 	$@ and croak $@;
# 
# 	return @toplevel;
# }

sub from_hash {
	my $proto = shift;
	my $row = shift;
	my $self = new $proto;

	(
		$self->{dbid}, $self->{refseq}, $self->{source},
		$self->{method}, $self->{start}, $self->{end},
		$self->{score}, $self->{strand}, $self->{phase},
		$self->{text_id}, $self->{name}, $self->{parent},
		$self->{importid}, $self->{assembly}, $self->{dataset},
		$self->{geneid}
	) = (
		$row->{id}, $row->{refseq}, $row->{source},
		$row->{method}, $row->{start}, $row->{end},
		$row->{score}, $row->{strand}, $row->{phase},
		$row->{text_id}, $row->{name}, $row->{parent},
		$row->{importid}, $row->{assembly}, $row->{dataset},
		$row->{geneid}
	);
	return $self;
}

sub to_text {
	my $self = shift;

	my $str = sprintf(
		"%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
		(
			map {
				field_escape($_);
			} ($self->{refseq}, coalesce($self->{source}, "."),
				$self->{method}, $self->{start}, $self->{end},
				coalesce($self->{score}, "."),
				coalesce($self->{strand}, "."),
				coalesce($self->{phase}, "."),
			)
		),
		$self->attr_string()
	);

	for my $child ($self->children) {
		$str .= $child->to_text();
	}

	return $str;
}

# rgc: add child GFF3Object
sub add_child {
	my $self = shift;
	my $child = shift;
	
	# link child back to us if it's not linked
	my @parents = $child->get_attr('Parent');
	unless (grep { $_ eq $self->{text_id} } @parents) {
		$child->append_attr(Parent => $self->{text_id});
	}

	# add child to our list
	push @{$self->{children}}, $child;

	return $self;
}

sub children {
	my $self = shift;
	return @{$self->{children}};
}

sub descendants {
	my $self = shift;
	my @children = $self->children();
	my @result = @children;
	my %seen = ();

	while(@children) {
		my $child = shift @children;
		# avoid cycles
		next if $seen{$child}++;
		push @children, $child->children();
		push @result, $child->children();
	}
	return @result;
}

sub _fix_source_method($$) {
	my $source = shift;
	my $method = shift;

	$source =~ s/^none$/./;

	$method =~ s/^contigs$/contig/;
	$method =~ s/^altcontig$/contig/;
	$method =~ s/^HSP$/match_part/;
	$method =~ s/^similarity$/match/;

	if ($source =~ /^exonerate/) {
		$method =~ s/^match$/transcript/;
		$method =~ s/^match_part$/exon/;
	}

	return ($source, $method);
}

# Which feature appears earlier?  Two features compare as equal only if they
# have exactly the same coordinates.  Note that no attempt is made to resolve
# the reference sequence (e.g. to convert contig to supercontig coordinates).
#
# Call as either $gffA->compare($gffB)  or  GFF3Object::compare($gffA, $gffB)
sub compare($$) {
	my ($a, $b) = @_;

	return (
		($a->refseq cmp $b->refseq)
		|| ($a->start <=> $b->start)
		|| ($a->end <=> $b->end)
		|| ($a->strand cmp $b->strand)
	);
}

1;
