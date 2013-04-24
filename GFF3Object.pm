#! /usr/bin/perl -w

package GFF3Object;
use strict;

sub field_escape($;$) {
	my $str = shift;
	my $esc_ws = shift;
	return uri_escape($str, "\\\"\x00-\x1f;=%&," . ($esc_ws ? " " : ""))
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

# sub from_json($) {
# 	my $proto = shift;
# 	my $json = shift;
# 
# 	my $self = new $proto;
# 
# 	my $hash = JSON::decode_json $json;
# 	my @subfeatures = @{$hash->{features}};
# 	delete $hash->{features};
# 
# 	for my $k (keys %$hash) {
# 		my $val = $hash->{$k};
# 		my @values;
# 		if (ref($val) eq "ARRAY") {
# 			@values = @{$val};
# 		} else {
# 			@values = ($val);
# 		}
# 		$self->set_attr($k, @values);
# 	}
# 	return $self;
# }

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

sub from_file {
	my $proto = shift;
	my $file = shift;
	my $filter = shift;

	$filter = sub { return @_; } unless defined $filter;

	my $fh;
	my $needs_close = 0;
	if (!defined $file) {
		$fh = \*STDIN;
	} elsif (ref $file) {
		$fh = $file;
	} else {
		$fh = new IO::File "$file", "r";
		$needs_close = 1;
	}

	my @toplevel;
	eval {
		my %by_id;
		while(<$fh>) {
			chomp;

			# Remove DOS line endings
			s/\r$//;

			# Avoid the FASTA portion of hybrid files
			last if /^##FASTA/;

			# Skip comments and blank lines
			next if /^#/ or /^\s*$/;

			my $gff = GFF3Object->from_text($_);

			# Filter the gff, skipping it if the filter returns undef
			next unless $filter->($gff);

			my $id = $gff->get_attr('ID');
			$by_id{$id} = $gff;

			# TODO: handle children that precede their parents
			my @parents = $gff->get_attr('Parent');
			for my $pid (@parents) {
				push @{$by_id{$pid}{children}}, $gff;
			}
			if (@parents == 0) {
				push @toplevel, $gff;
			}
		}
	};
	close $fh if $needs_close;
	$@ and croak $@;

	return @toplevel;
}

sub from_row {
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
#	my $attrdt = connection->execute_data_table(q/
#		select gkey, value from gff3_extra
#		where gff3_id = ?
#		/, $self->{dbid});
#
#	for my $atrow ($attrdt->rows) {
#		my ($key, $value) = ($atrow->{gkey}, $atrow->{value});
#		if (!exists($self->{attrs}{$key})) {
#			$self->{attrs}{$key} = [];
#		}
#		push @{$self->{attrs}{$key}}, $value;
#	}
	return $self;
}

#sub from_db {
#	my $proto = shift;
#	my %keys = @_;
#	my $self = new $proto;
#
#	my $dt = undef;
#	if(exists $keys{dbid}) {
#		$dt = connection->execute_data_table(q/
#			select gff3.*, geneid from gff3
#			left join gff3gene on gffid = gff3.id
#			where id = ?
#		/, $keys{dbid});
#	} elsif (exists $keys{textid}) {
#		if (exists $keys{dataset}) {
#			$dt = connection->execute_data_table(q/
#				select gff3.*, geneid from gff3
#				left join gff3gene on gffid = gff3.id
#				where text_id = ? and assembly = ? and dataset = ?/,
#				$keys{textid}, $keys{assembly} || 1,
#				$keys{dataset}
#			);
#		} else {
#			$dt = connection->execute_data_table(q/
#				select * from gff3
#				where text_id = ?/,
#				$keys{textid}
#			);
#		}
#	} else {
#		croak "GFF3::from_db: require dbid or textid";
#	}
#
#	for my $row ($dt->rows) {
#		return $proto->from_row($row);
#	}
#	return undef;
#}
#
#sub TO_JSON($) {
#	my $self = shift;
#	my $ref = {};
#
#	for my $k ($self->base_attrs, "parent") {
#		$ref->{$k} = $self->{$k};
#	}
#
#	for my $k (keys %{$self->{attrs}}) {
#		my $val = $self->{attrs}{$k};
#		# treat single-element arrays as simple scalars
#		if (ref $val eq "ARRAY" and @$val == 1) {
#			$ref->{$k} = $val->[0];
#		} else {
#			$ref->{$k} = $val;
#		}
#	}
#	# Parent is stored twice; keep only the one from attrs.
#	delete $ref->{parent};
#	$ref;
#}
#
sub to_text($;$) {
	my $self = shift;
	my $recursive = shift;

	$recursive and return $self->_to_text_rec({});

	return sprintf(
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
}

sub _to_text_rec($$) {
	my $self = shift;
	my $seen = shift;
	return "" if $seen->{$self->id}++;

	my $text = $self->to_text();
	for my $child ($self->children) {
		$text .= $child->_to_text_rec($seen);
	}
	return $text;
}

# $gff->to_db([assembly, dataset])
#sub to_db {
#	my $self = shift;
#	my ($assembly, $dataset) = @_;
#
#	$self->{assembly} = $assembly || 1;
#	$self->{dataset} = $dataset || 0;
#
#	my $conn = connection("update");
#	my $dbid = $self->{dbid};
#
#	if (defined $dbid) {
#		$dbid = $conn->execute_insertupdate(
#			"gff3", {
#				id => $dbid,
#				refseq => $self->{refseq},
#				source => $self->{source},
#				method => $self->{method},
#				start => $self->{start},
#				end => $self->{end},
#				score => $self->{score},
#				strand => coalesce($self->{strand}, "."),
#				phase => coalesce($self->{phase}, "."),
#				text_id => $self->{text_id},
#				name => $self->{name},
#				parent => $self->{parent},
#				importid => $self->{importid},
#				assembly => $self->{assembly},
#				dataset => $self->{dataset},
#			}, [ "id" ]
#		);
#		if ($dbid != $self->{dbid}) {
#			$self->{dbid} = $dbid;
#			warn "GFF3::to_db: mismatching dbids: $self->{dbid} provided, $dbid returned";
#		}
#	} else {
#		$dbid = $conn->execute_insertupdate(
#			"gff3", {
#				refseq => $self->{refseq},
#				source => $self->{source},
#				method => $self->{method},
#				start => $self->{start},
#				end => $self->{end},
#				score => $self->{score},
#				strand => $self->{strand},
#				phase => $self->{phase},
#				text_id => $self->{text_id},
#				name => $self->{name},
#				parent => $self->{parent},
#				importid => $self->{importid},
#				assembly => $self->{assembly},
#				dataset => $self->{dataset},
#			}, [ "id:auto_ignored", "text_id", "assembly", "dataset" ]
#		);
#		$self->{dbid} = $dbid;
#	}
#
#	defined($dbid) 
#		or croak "GFF3::to_db ins/upd failed on " . $self->{text_id};
#
#	# Clear existing attrs
#	$conn->execute_delete("gff3_extra", {gff3_id => $dbid});
#
#	# Set attrs
#	for my $attr_key (keys %{$self->{attrs}}) {
#		my $attr_val = $self->{attrs}{$attr_key};
#		if (ref($attr_val) ne "ARRAY") {
#			$attr_val = defined($attr_val) ? [ ] : [ $attr_val ];
#		}
#		for my $val (@$attr_val) {
#			$conn->execute_insert("gff3_extra",
#				{ gff3_id => $dbid, gkey => $attr_key, value => $val },
#				"id:auto"
#			);
#		}
#	}
#
#	return $dbid;
#}


sub to_group {
	my $self = shift;
	# The fsource/gclass will be "$import_type:N" for some N.
	my $import_type = shift;
	# Track name; looked up by FPD::App::Gbrowse::import_group
	my $track = shift;

	my @descs = $self->descendants();
	my @features = ();

	my %seen;

	# For each descendant of $self, add it as a feature to the list
	# @features.
	foreach my $item ($self, @descs) {
		# Convert to reference (supercontig) coordinates.
		my ($ref, $start, $stop) = $item->ref_coords();
		my $key = "$ref-$start-$stop-" . $item->{method};

		my $feat = {
			fref => $ref, fstart => $start, fstop => $stop,
			fmethod => $item->{method},
			# don't set fsource, import_group just overwrites it
			fscore => $item->{score},
			fstrand => $item->{strand},
			fphase => $item->{phase},
		};

		# Constrain to set of allowable values.
		if ($feat->{fstrand} !~ /^[+-]$/) {
			delete $feat->{fstrand};
		}
		if ($feat->{fphase} !~ /^[012]$/) {
			delete $feat->{fphase};
		}

		# Skip duplicates
		if (!exists $seen{$key}) {
			$seen{$key} = $feat;
			push @features, $feat;
		}

	}

	# Build a group with a name and the list of features.  The gclass
	# will be the same as the fsource.
	return import_group($import_type, {
		gname => "$import_type.$self->{dbid} $self->{name}",
		features => \@features
	}, $track);
}


# sub to_row {
# 	my $self = shift;
# 
# 	my %cvals = ();
# 	my @cols = ();
# 
# 	for my $attr ($self->base_attrs, "parent") {
# 		push @cols, $attr;
# 		$cvals{$attr} = $self->{$attr};
# 	}
# 	for my $attr (sort keys %{$self->{attrs}}) {
# 		push @cols, $attr;
# 		$cvals{$attr} = $self->{attrs}{$attr};
# 	}
# 	# Make sure "5prime" attributes come before correspoding "3prime" attributes
# 	for my $i (1 .. $#cols) {
# 		if ($cols[$i] =~ /^(.*)5prime$/ and $cols[$i-1] =~ /^${1}3prime$/) {
# 			@cols[$i-1, $i] = @cols[$i, $i-1];
# 		}
# 	}
# 
# 	my $table = FPD::Data::DataTable->new(\@cols);
# 	return $table->new_row(\%cvals);
# }

# rgc: add child GFF3Object
sub add_child($$) {
	my $self = shift;
	my $child = shift;
	push @{$self->children}, $child;
	return $self;
}

sub children {
	my $self = shift;

	# unless ($self->{children}) {
	# 	$self->{children} = [];

	# 	return () unless defined $self->{dbid};
	# 	my $dt = connection->execute_data_table(q/
	# 		select gff3.id from gff3
	# 		  inner join gff3_extra on gff3_id = gff3.id
	# 		where gkey = 'Parent' and value = ?
	# 		  and assembly = ? and dataset = ?
	# 	/, $self->{text_id}, $self->{assembly}, $self->{dataset});
	# 
	# 	for my $row ($dt->rows) {
	# 		push @{$self->{children}}, $self->from_db(dbid => $row->id);
	# 	}
	# }
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

#sub from_group($$;$) {
#	my $proto = shift;
#	my $class = ref($proto) || $proto;
#
#	my ($group, $opts);
#
#	if (@_ == 1) {
#		$opts = {};
#		$group = shift;
#	} else {
#		$opts = shift;
#		$group = shift;
#	}
#
#	# TODO: support extra track-specific data (alignment for blast, etc).
#	my $track = $group->{gb_track}->{feature};
#	my @ftypes = split " ", $track;
#	my @top_methods = ();
#	for my $ft (@ftypes) {
#		my ($method, $source) = split /:/, $ft, 2;
#		push @top_methods, $method;
#	}
#
#	my $feat_dt = connection("gbrowse")->execute_data_table(qq/
#		select * from fdata
#		inner join ftype on ftype.ftypeid = fdata.ftypeid
#		where gid = ?/, $group->gid
#	);
#
#	my $topgff = undef;
#	my $featct = 1;
#	for my $feat ($feat_dt->rows()) {
#		my $gff = $class->new();
#
#		if ($opts->{contig}) {
#			($gff->{refseq}, $gff->{start}, $gff->{end}) = convert_reference_region_to_contig_region(@$feat{qw/fref fstart fstop/});
#		} else {
#			$gff->{refseq} = $feat->{fref};
#			$gff->{start} = $feat->{fstart};
#			$gff->{end} = $feat->{fstop};
#		}
#
#		($gff->{source}, $gff->{method}) = _fix_source_method($feat->{fsource}, $feat->{fmethod});
#		$gff->{score} = $feat->{fscore};
#		$gff->{strand} = $feat->{fstrand};
#		$gff->{phase} = $feat->{fphase};
#		
#		my $name = $group->gname;
#		# Remove database link
#		if($name =~ s/^((?:exonerate|t?blast\w+|ipr)\.[0-9]+) //) {
#			$name .= " $1";
#		}
#
#		if (!defined $topgff && in_list($feat->{fmethod}, @top_methods)) {
#			$topgff = $gff;
#			$gff->set_attr("Name", $name);
#			$gff->set_attr("ID", $name);
#		} else {
#			my $partname = $name;
#			$partname =~ s/( |$)/:$featct$1/;
#			++$featct;
#
#			$gff->set_attr("ID", $partname);
#			$gff->set_attr("Parent", $name);
#		}
#
#		$feat->{gff} = $gff;
#	}
#
#	return map { $_->{gff} } $feat_dt->rows();
#}

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

# $transcript->cds_annotate([$orfstart, $orfend]);
#
# Annotate $transcript (which must contain exon children) with CDS features
# based on the supplied ORF (or the transcript's longest forward open reading
# frame, if no ORF was specified).  Any existing CDS children of $transcript
# are removed.
#
# $orfstart and $orfend are expressed in spliced transcript coordinates relative
# to the transcript's 5' end; introns are not counted, and the 5' base of the 5'
# exon is 1.
#sub cds_annotate($;$$) {
#	my $self = shift;
#	my ($orfstart, $orfend) = @_;
#
#	# Remove CDS children from $self; sort the exon children into @exons.
#	# Note that the CDS children are NOT removed from the database, though
#	# their parent attributes are modified.
#	my (@exons, @children, @cdses);
#	for my $child ($self->children) {
#		if ($child->method eq "exon") {
#			push @exons, $child;
#			push @children, $child;
#		} elsif ($child->method eq "CDS") {
#			$child->set_attr(Parent => grep {
#					$_ ne $self->id
#				} $child->get_attr('Parent')
#			);
#		} else {
#			push @children, $child;
#		}
#	}
#	@exons = sort GFF3Object::compare @exons;
#
#	my $revcomp = ($self->strand eq '-');
#
#	if ($revcomp) {
#		@exons = reverse @exons;
#	}
#
#	# Number of bases remaining until the beginning/end of the ORF.
#	my ($untilorf,$untilend);
#
#	# TODO: support two-parameter call ($orfstart but no $orfend). 
#	# Semantics would be to extract the ORF starting from that position.
#	if (defined $orfstart) {
#		# $orfstart/end are 1-indexed
#		$untilorf = $orfstart - 1;
#		$untilend = $orfend - 1;
#	} else {
#		# Extract the sequence and find the ORF.
#		my $seq = join '', map {
#			my $seq = FPD::Sequence::extract_sequence(
#				$_->refseq, $_->start, $_->end
#			);
#			$revcomp ? FPD::Sequence::revcomp($seq) : $seq;
#		} @exons;
#		my $orf = FPD::Sequence::find_long_orf $seq;
#
#		# No ORF
#		defined $orf->{start} or return $self;
#
#		$untilorf = $orf->{start};
#		$untilend = $orf->{end};
#	}
#
#	# Convert (feature, start_offset, end_offset) into (start, end), where
#	# the offsets are relative to the 5' end of the feature, but the results
#	# are relative to the reference sequence (with start <= end).  Offsets
#	# beyond the bounds of the feature (start_offset < 0,
#	# end_offset > length-1) are clipped to those bounds.
#	#
#	my $offset = sub { # Closure over $revcomp
#		my ($feat, $stoff, $enoff) = @_;
#
#		# Clip to feature
#		$stoff = 0 if $stoff < 0;
#		$enoff = $feat->len - 1 if $enoff >= $feat->len;
#
#		my $st = $revcomp ? ($feat->end - $enoff) : ($feat->start + $stoff);
#		my $en = $revcomp ? ($feat->end - $stoff) : ($feat->start + $enoff);
#		return ($st, $en);
#	};
#
#	my $phase = 0;
#	my $cdsno = 0;
#	my $cdslen = 0;
#	EXON: for my $exon (@exons) {
#		if ($untilorf >= $exon->len) {
#			# ORF has not yet started; do nothing
#		} elsif ($untilend < 0) {
#			# ORF has already ended; we are done
#			last EXON;
#		} else {
#			# Clone this exon into a CDS
#			my $cds = $exon->clone();
#			$cds->set_attr(method => 'CDS');
#			$cds->set_attr(ID => $self->id . ":cds:" . ++$cdsno);
#
#			# The exon may have been multiply-parented, but the
#			# CDS should not (yet) be.
#			$cds->set_attr(Parent => $self->id);
#
#			# Clip ORF region inside this exon, and make that the
#			# CDS region.
#			my ($st, $en) = $offset->($exon, $untilorf, $untilend);
#			$cds->set_attr(start => $st);
#			$cds->set_attr(end => $en);
#
#			$cdslen += ($en - $st + 1);
#
#			$cds->set_attr(phase => $phase);
#			$phase = ($phase - $cds->len) % 3;
#
#			push @cdses, $cds;
#		}
#
#		$untilorf -= $exon->len;
#		$untilend -= $exon->len;
#	}
#	$self->set_attr(cdslen => $cdslen/3);
#	$self->{children} = [@children, @cdses];
#}

1;
