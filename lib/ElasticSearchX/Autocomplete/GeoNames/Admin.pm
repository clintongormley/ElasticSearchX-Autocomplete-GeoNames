package ElasticSearchX::Autocomplete::GeoNames::Admin;

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use List::MoreUtils qw(uniq);
use Carp;
use Geo::Distance();

our %Place_Ranks = (
    PCLI  => 10,    # independent political entitity
    ADM1  => 9,     # first order admin
    PPLC  => 9,     # capital of political entity
    PCLD  => 8,     # dependent political entitity
    PCLIX => 8,     # section of independent political entitity
    TERR  => 7,     # territory
    PCLF  => 8,     # freely associated state
    PCLS  => 7,     # semi-independent political entitity
    ADM2  => 7,     # second order admin
    PPLA  => 6,     # seat of first order administrative division
    ADM3  => 5,     # third order admin
    PPLA2 => 4,     # seat of second order administrative division
    PPLA3 => 3,     # seat of third order administrative division
    ADM4  => 2,     # fourth order admin
    PPL   => 1,     # populated place
    PPLX  => 1,     # section of populated place
);

our %Merge_Places = (
    ADM3  => 1,
    ADM4  => 1,
    PPL   => 1,
    PPLA  => 1,
    PPLA2 => 1,
    PPLA3 => 1,
    PPLC  => 1,
    PPLX  => 1,
);

use ElasticSearchX::Autocomplete::Util qw(_create_accessors _params _debug );

__PACKAGE__->_create_accessors(
    ['debug'],
    ['JSON'],
    ['geonames'],
    ['place_ranks'],
    ['merge_places'],
    [ 'langs',          q(['en']) ],
    [ 'altnames_index', q('geonames_temp') ],
    [ 'altnames_type',  q('altname') ]
);

#===================================
sub new {
#===================================
    my ( $class, $params ) = _params(@_);

    my $self = {
        _place_ranks  => {%Place_Ranks},
        _merge_places => {%Merge_Places},
        _geo_distance => Geo::Distance->new(),
        _debug        => 0
    };

    bless $self, $class;
    $self->$_( $params->{$_} ) for keys %$params;
    return $self;
}

#===================================
sub indexer {
#===================================
    my $self        = shift;
    my $auto_create = !shift();
    unless ( $self->{_indexer} ) {
        my $auto = $self->geonames->auto;

        my $indexer = eval { $auto->indexer( edit => 1 ) };
        if ( !$indexer ) {
            return unless $auto_create;
            $indexer = $auto->indexer();
            my $type_indexer = $indexer->type('place');
            $type_indexer->init;
        }

        $indexer->cleanup(0);
        $self->{_indexer} = $indexer;
    }
    return $self->{_indexer};
}

#===================================
sub type_indexer {
#===================================
    my $self = shift;
    return $self->{_type_indexer} ||= $self->indexer->type('place');
}

#===================================
sub index_places {
#===================================
    my $self     = shift;
    my $geonames = $self->geonames;

    my $index = $geonames->index;
    my $type  = $geonames->type;

    while ( my $filename = shift @_ ) {
        $self->_debug( 1, "Loading file $filename" );
        my $source = $self->_open_csv($filename);
        while ($source) {
            my $places = $self->_parse_places($source);
            undef $source unless $source->{row};

            $self->_debug( 2, ' - Adding alternative names' );
            $self->_add_altnames($places);

            $self->_debug( 2, ' - Removing duplicates' );
            my $dups = $self->_remove_duplicates($places);

            $self->_debug( 2, ' - Indexing places' );
            $self->_index_places($places);

            $self->_debug( 2, ' - Indexing duplicates' );
            $self->_index_dups($dups);

        }
        $self->_debug( 1, "Finished processing file $filename" );

    }
    $self->indexer->deploy( replicas => '', optimize => 0 );
    $self->_debug( 1, 'Done' );
}

#===================================
sub deploy {
#===================================
    my ( $self, $params ) = _params(@_);
    $params = {
        optimize => 1,
        replicas => '0-all',
        %$params
    };
    $self->indexer->deploy($params);
}

#===================================
sub _parse_places {
#===================================
    my $self   = shift;
    my $source = shift;

    my ( %adm, %places, $last_country );

    my $ranks = $self->place_ranks;
    my $csv   = $source->{csv};
    my $fh    = $source->{fh};

    while ( my $row = ( delete $source->{row} || $csv->getline($fh) ) ) {

        my $country = lc $row->[8] or next;
        if ($last_country) {
            if ( $country ne $last_country ) {
                $source->{row} = $row;
                last;
            }
        }
        else {
            $last_country = $country;
            $self->_debug( 1, "Loading places for country: ", $country );
        }

        my $class = $row->[7];
        my $rank  = $ranks->{$class}
            or next;

        my $id = $row->[0];
        my @codes = grep {$_} @{$row}[ 10 .. 13 ];

        my %data = (
            id       => $id,
            name     => $row->[1],
            location => { lon => $row->[4], lat => $row->[5] },
            country  => $country,
            codes    => \@codes,
            rank     => $rank,
            class    => $class,
        );

        my $code = join '_', @codes;
        if ( $row->[6] eq 'A' ) {
            if ( $code eq '00' ) {
                $code = $country;
                $data{is_country}++ unless $adm{$code};
            }
            $adm{$code} ||= \%data;
            pop @codes;
        }

        $places{$id} = \%data;
    }

    for my $id ( keys %places ) {
        my $place = $places{$id};
        my @codes = @{ delete $place->{codes} };

        my @ids;
        while (@codes) {
            if ( my $parent = $adm{ join '_', @codes } ) {
                push @ids, $parent->{id};
            }
            pop @codes;
        }

        my $country = $adm{ $place->{country} }
            or croak "Missing country for place: "
            . $self->geonames->JSON->encode($place);

        push @ids, $country->{id}
            unless $country->{id} == $id;

        $place->{parent_ids} = \@ids;
    }

    carp( $csv->error_diag ) unless $source->{row} || $csv->eof;

    return \%places;
}

#===================================
sub _remove_duplicates {
#===================================
    my $self   = shift;
    my $places = shift;

    my ( %names, %duplicates );

    my $langs = $self->langs;
    for my $id ( keys %$places ) {
        my $place = $places->{$id};
        push @{ $names{ $place->{name} } }, $id;
        for my $lang (@$langs) {
            if ( my $name = $place->{names}{$lang}{long} ) {
                push @{ $names{$name} }, $id;
            }
        }
    }

    $self->_dedup( $places, \%duplicates, $_ ) for values %names;

    for my $dup ( values %duplicates ) {
        my $dup_of = $dup->{dup_of};
        while (1) {
            last if $places->{$dup_of};
            $dup_of = $duplicates{$dup_of}->{dup_of};
        }
        $dup->{dup_of} = $dup_of;
    }

    for my $place ( values %$places ) {
        my @new_parents;
        for my $parent_id ( @{ $place->{parent_ids} } ) {
            if ( $places->{$parent_id} ) {
                push @new_parents, $parent_id;
            }
            else {
                push @new_parents, $duplicates{$parent_id}->{dup_of};
            }
        }
        $place->{parent_ids} = \@new_parents;
        my $ancestor_id = $place->{ancestor_id};
        $place->{ancestor_id} = $duplicates{$ancestor_id}{dup_of}
            if $ancestor_id
                && !$places->{$ancestor_id}

    }
    return \%duplicates;
}

#===================================
sub _dedup {
#===================================
    my $self       = shift;
    my $places     = shift;
    my $duplicates = shift;

    my @uniques = grep {$_} map { $places->{$_} } uniq @$_;
    return unless @uniques > 1;
    my $merge_places = $self->merge_places;

    my ( %tree, @deduped, %highest );

    for my $place ( sort { $b->{rank} <=> $a->{rank} } @uniques ) {
        my $merge;

        # merge with parent if they have the same name
        if ( $merge_places->{ $place->{class} } ) {
            my ($parent) = grep { $_ && $merge_places->{ $_->{class} } }
                map { $places->{$_} } @{ $place->{parent_ids} };
            if ( $parent && $place->{name} eq $parent->{name} ) {
                $parent->{rank} = $place->{rank}
                    if $place->{rank} > $parent->{rank};
                $merge = $parent;
            }
        }
        unless ($merge) {
            my $branch = \%tree;
            my $level  = 0;
            for my $id ( reverse @{ $place->{parent_ids} } ) {
                next unless $places->{$id};
                $branch = $branch->{$id} ||= {};
                $level++;
            }

            # merge with duplicate name in same area
            $merge = $branch->{place};

           # merge with descendants in the same tree with similar rank/latlong
            unless ($merge) {
                for my $desc ( grep $_, $self->_descendants($branch) ) {
                    next if abs( $desc->{rank} - $place->{rank} ) > 2;
                    my $distance = $self->{_geo_distance}->distance(
                        'kilometer',
                        @{ $place->{location} }{ 'lon', 'lat' },
                        @{ $desc->{location} }{ 'lon',  'lat' },
                    );
                    next if $distance > 5;
                    $merge = $desc;
                    last;
                }
            }
            unless ($merge) {
                push @deduped, $place;
                $branch->{place} = $place;
                push @{ $highest{$level} }, $place;
                next;
            }
        }
        delete $places->{ $place->{id} };
        $place->{dup_of} = $merge->{id};
        $duplicates->{ $place->{id} } = $place;
    }

    $self->_assign_unique_ancestor( \%tree );

    # the highest level duplicate doesn't require an ancestor
    # to have a unique name
    my ($exclude) = map { $highest{$_} } sort keys %highest;
    return if @$exclude > 1;
    ($exclude) = @$exclude;
    return if $exclude->{rank} < 5;
    delete $exclude->{ancestor_id}
        if 1 == grep { $_->{ancestor_id} == $exclude->{ancestor_id} }
            @deduped;
}

#===================================
sub _descendants {
#===================================
    my $self   = shift;
    my $branch = shift;
    my $place  = $branch->{place} || '';
    return $place, map { $self->_descendants($_) }
        grep { $_ ne $place } values %$branch;
}

#===================================
sub _assign_unique_ancestor {
#===================================
    my $self        = shift;
    my $tree        = shift;
    my $ancestor_id = shift;
    my $place       = delete $tree->{place};
    if ($place) {
        $place->{ancestor_id} = $ancestor_id;
    }

    my @ids = keys %$tree
        or return;
    $ancestor_id = 0 if $place or @ids > 1;
    $self->_assign_unique_ancestor( $tree->{$_}, $ancestor_id || $_ )
        for @ids;
}

#===================================
sub _add_altnames {
#===================================
    my $self    = shift;
    my $places  = shift;
    my @all_ids = keys %$places;
    my %names;

    my $alt_index = $self->altnames_index;
    my $alt_type  = $self->altnames_type;
    my $es        = $self->geonames->es;
    my $langs     = [ @{ $self->langs }, '' ];    # '' to include the default

    while (@all_ids) {
        my @ids = splice( @all_ids, 0, 1000 );
        my $scroll = $es->scrolled_search(
            index  => $alt_index,
            type   => $alt_type,
            scroll => '5m',
            queryb => {
                -filter => {
                    place_id => \@ids,
                    lang     => [ { '=' => $langs }, { '=' => undef } ],
                },
            },
            sort => [
                { place_id  => 'asc' },
                { lang      => 'asc' },
                { preferred => 'desc' }
            ],
            size => 1000,
        );

        while ( my $doc = $scroll->next() ) {
            my $src   = $doc->{_source};
            my $id    = $src->{place_id};
            my $lang  = $src->{lang} || 'default';
            my $name  = $src->{name};
            my $entry = $names{$id}{$lang} ||= {};
            $entry->{long} ||= $name
                if $lang ne 'default' || $src->{preferred};
            $entry->{short} ||= $name if $src->{short};
            $entry->{alts}{$name}++;
        }
    }

    for my $id ( keys %names ) {
        my $names = $names{$id};
        my $place = $places->{$id};
        $place->{name}       = $names->{default}{long}  || $place->{name};
        $place->{short_name} = $names->{default}{short} || $place->{name};
        for my $lang ( values %$names ) {
            my $alts = delete $lang->{alts};
            delete $alts->{ $lang->{$_} || '' } for qw(short long);
            $lang->{alts} = [ keys %$alts ] if keys %$alts;
        }
        $place->{names} = $names;

    }
}

#===================================
sub _index_places {
#===================================
    my $self   = shift;
    my $places = shift;

    my @phrases;
    my $i            = 0;
    my $langs        = $self->langs;
    my $type_indexer = $self->type_indexer;

    for my $id ( keys %$places ) {
        my $place = $places->{$id};

        my @parent_ids = grep { $places->{$_} } @{ $place->{parent_ids} };
        my $country_id  = $parent_ids[-1]       || 0;
        my $ancestor_id = $place->{ancestor_id} || 0;
        my @ancestors = map { $places->{$_} }
            uniq grep {$_} ( $ancestor_id, $country_id );

        for my $lang (@$langs) {
            $i++;
            push @phrases,
                {
                doc_id     => $place->{id} . '_' . $lang,
                place_id   => $place->{id},
                rank       => { $lang => $place->{rank} },
                location   => $place->{location},
                parent_ids => \@parent_ids,
                $self->_labels( $lang, $place, @ancestors )
                };
        }

        if ( @phrases >= 4950 ) {
            $type_indexer->index_phrases(
                phrases    => \@phrases,
                no_refresh => 0
            );
            @phrases = ();
        }
    }
    $type_indexer->index_phrases( phrases => \@phrases );
    $self->_debug( 1, "Indexed $i places" );
}

#===================================
sub _labels {
#===================================
    my $self    = shift;
    my $lang    = shift;
    my $place   = shift;

    my $parents = join ', ', '', map {
               $_->{names}{$lang}{short}
            || $_->{names}{$lang}{long}
            || $_->{short_name}
            || $_->{name}
    } @_;

    my $default    = $place->{names}{default};
    my $lang_names = $place->{names}{$lang};

    my $name
        = $lang_names->{long}
        || $lang_names->{short}
        || $place->{short_name}
        || $place->{name};

    my $full = $name . $parents;

    my %variations = map { $_ => 1, $_ . $parents => 1 } $name,
        grep {$_} $lang_names->{short},
        $lang_names->{long},
        @{ $lang_names->{alts} || [] },
        $default->{short},
        $default->{long},
        @{ $default->{alts} || [] };

    my $type_indexer = $self->type_indexer;
    my @tokens = map { { tokens => [ $type_indexer->tokenize($_) ] } }
        keys %variations;
    return (
        label       => $full,
        label_short => $name,
        tokens      => \@tokens,
    );
}

#===================================
sub _index_dups {
#===================================
    my $self = shift;
    my $dups = shift;

    my @phrases;
    my $i            = 0;
    my $langs        = $self->langs;
    my $type_indexer = $self->type_indexer;

    for my $id ( keys %$dups ) {
        my $place = $dups->{$id};

        for my $lang (@$langs) {
            $i++;
            push @phrases,
                {
                doc_id   => $place->{id} . '_' . $lang,
                place_id => $place->{id},
                dup_of   => $place->{dup_of},
                rank     => { $lang => 0 },
                };
        }

        if ( @phrases >= 4950 ) {
            $type_indexer->index_phrases(
                phrases    => \@phrases,
                no_refresh => 0
            );
            @phrases = ();
        }
    }
    $type_indexer->index_phrases( phrases => \@phrases );
    $self->_debug( 1, "Indexed $i duplicates" );
}

#===================================
sub delete {
#===================================
    my $self    = shift;
    my $indexer = $self->indexer('no_auto')
        or croak "No index to delete";
    $indexer->delete();
}

#===================================
sub index_altnames {
#===================================
    my $self = shift;

    my $es    = $self->geonames->es;
    my $debug = $self->debug;

    $self->_debug( 1, 'Indexing alternative names' );
    my ( $index, $type ) = $self->init_altnames_index('check_exists');

    local $| = 1 if $debug;
    my $i = 0;

    while ( my $filename = shift() ) {
        my $source = $self->_open_csv($filename);

        my $csv = $source->{csv};
        my $fh  = $source->{fh};

        my @data;
        while (1) {
            my $row = $csv->getline($fh);
            carp( $csv->error_diag ) unless $row || $csv->eof;
            if ( !$row || @data == 5000 ) {
                my $result = $es->bulk_index( \@data );
                if ( my $errors = $result->{errors} ) {
                    croak( "Error indexing alternate names: \n"
                            . $self->geonames->JSON->encode($errors) );
                }
                $i += @data;
                @data = ();
                print "." if $debug;
            }
            last unless $row;

            # skip links etc
            next if length( $row->[2] ) > 3;

            push @data,
                {
                index => $index,
                type  => $type,
                id    => $row->[0],
                data  => {
                    place_id  => $row->[1],
                    lang      => $row->[2] || '',
                    name      => $row->[3],
                    preferred => $row->[4] || 0,
                    short     => $row->[5] || 0
                }
                };
        }
        print "\n" if $debug;
    }
    $es->refresh_index( index => $index );
    $self->_debug( 1, 'Indexed ', $i, ' altnames' );

}

#===================================
sub init_altnames_index {
#===================================
    my $self         = shift;
    my $check_exists = shift;

    my $es = $self->geonames->es;

    my $index = $self->altnames_index;
    my $type  = $self->altnames_type;
    if ( $check_exists && eval { $es->mapping( index => $index )->{$index}{$type} } ) {
        $self->_debug( 1, "Altnames index exists: $index/$type" );
    }
    else {
        $self->_debug( 1, "Creating altnames index: $index/$type" );
        $es->create_index(
            index    => $index,
            settings => {
                number_of_replicas => 0,
                number_of_shards   => 1,
                refresh_interval   => -1
            },
            mappings => {
                $type => {
                    _all       => { enabled => 0 },
                    properties => {
                        place_id  => { type => 'integer' },
                        preferred => { type => 'integer' },
                        short     => { type => 'boolean' },
                        name      => { type => 'string', index => 'no' },
                        lang => { type => 'string', index => 'not_analyzed' },
                    }
                }
            }
        );

        $es->cluster_health(
            index           => $index,
            wait_for_status => 'green'
        );
    }
    return ( $index, $type );
}

#===================================
sub delete_altnames_index {
#===================================
    my $self  = shift;
    my $es    = $self->geonames->es;
    my $index = $self->altnames_index;

    $self->_debug( 1, 'Deleting altnames index : ', $index );
    $es->delete_index( index => $index );
}

#===================================
sub _open_csv {
#===================================
    my $self     = shift;
    my $filename = shift
        or croak("No CSV filename specified");

    $self->_debug( 2, ' - Opening CSV file ', $filename, ' for reading' );

    open my $fh, '<:encoding(utf8)', $filename
        or die "Couldn't open $filename for reading : $!";

    my $class
        = eval { require Text::CSV_XS } ? 'Text::CSV_XS'
        : eval { require Text::CSV }    ? 'Text::CSV'
        :   croak("Please install Text::CSV_XS or Text::CSV");

    my $csv = $class->new( {
            binary      => 1,
            sep_char    => "\t",
            escape_char => undef,
            quote_char  => undef,
        }
    );
    return { fh => $fh, csv => $csv };
}

=head1 NAME

ElasticSearchX::Autocomplete::GeoNames::Admin

=head1 DESCRIPTION

To follow

=head1 SEE ALSO

L<ElasticSearchX::Autocomplete::GeoNames>

=head1 AUTHOR

Clinton Gormley, E<lt>clinton@traveljury.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Clinton Gormley

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut

1
