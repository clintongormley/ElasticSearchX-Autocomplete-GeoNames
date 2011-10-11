package ElasticSearchX::Autocomplete::GeoNames;

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use ElasticSearchX::Autocomplete();
use ElasticSearchX::Autocomplete::Util qw(
    _create_accessors _params
    _debug _try_cache cache_key
);

use Carp;

our $VERSION = '0.05';

__PACKAGE__->_create_accessors(
    ['cache'],
    ['debug'],
    ['JSON'],
    ['auto'],
    ['auto_type'],
    [ 'es',    q(croak "Missing required param 'es'") ],
    [ 'index', q('geonames') ],
    [ 'type',  q('place') ],
);

#===================================
sub new {
#===================================
    my ( $proto, $params ) = _params(@_);
    my $class = ref $proto || $proto;

    my $self = { _debug => 0 };
    bless $self, $class;

    my %auto_params;
    for ( keys %$params ) {
        if ( $self->can($_) ) {
            $self->$_( $params->{$_} );
        }
        else {
            $auto_params{$_} = $params->{$_};
        }
    }

    $self->_init( \%auto_params );

    return $self;
}

#===================================
sub _init {
#===================================
    my $self   = shift;
    my $params = shift;
    my $auto   = ElasticSearchX::Autocomplete->new(
        ( map { $_ => $self->$_ } qw(index cache debug es ) ),
        types => {
            $self->type => {
                custom_fields => {
                    parent_ids  => { type => 'integer' },
                    place_id    => { type => 'integer' },
                    label_short => { type => 'string', index => 'no' },
                    dup_of      => { type => 'integer' },
                },
                geoloc       => 1,
                multi_tokens => 1,
                formatter    => \&_format_results,
                %$params,
            }
        }
    );
    $self->auto($auto);
    $self->auto_type( $auto->type( $self->type ) );
    $self->JSON( $auto->JSON );
}

#===================================
sub suggest {
#===================================
    my $self = shift;
    my ( $phrase, $params ) = _params(@_);
    $params->{context} = delete $params->{lang};
    return $self->auto_type->suggest( $phrase, $params );
}

#===================================
sub suggest_json {
#===================================
    my $self = shift;
    my ( $phrase, $params ) = _params(@_);
    $params->{context} = delete $params->{lang};
    return $self->auto_type->suggest_json( $phrase, $params );
}

our $as_json;
#===================================
sub get_place {
#===================================
    my ( $self, $params ) = _params(@_);
    my $type = $self->auto_type;
    $params->{context} = $type->clean_context( $params->{lang} );
    $params->{index}   = $type->index;
    $params->{type}    = $type->name;

    return $self->_try_cache( '_get_place', $params, $as_json );
}

#===================================
sub get_place_json {
#===================================
    my $self = shift;
    local $as_json = 1;
    return $self->get_place(@_);
}

#===================================
sub _get_place {
#===================================
    my $self   = shift;
    my $params = shift;

    my $id   = $params->{id}   or croak "No place ID passed to get_place";
    my $lang = $params->{lang} or croak "No lang passed to get_place";

    my $doc = $self->es->get(
        index          => $params->{index},
        type           => $params->{type},
        id             => "${id}_${lang}",
        ignore_missing => 1
    ) or return undef;

    if (my $dup_id = $doc->{_source}{dup_of}) {
        return $self->_get_place({%$params, id=>$dup_id});
    }
    return _localise_place( $lang, $doc );
}

#===================================
sub _localise_place {
#===================================
    my $lang = shift;
    my $doc  = shift;
    my $src  = $doc->{_source};

    return {
        id   => $src->{place_id},
        lang => $lang,
        map { $_ => $src->{$_} }
            qw(location parent_ids rank label label_short),
    };
}

#===================================
sub _get_place_by_id {
#===================================
    my $self   = shift;
    my $params = shift;
    my $id     = $params->{id};
    my $lang   = $params->{lang};

    return $self->es->get(
        index          => $params->{index},
        type           => $params->{type},
        id             => "${id}_${lang}",
        ignore_missing => 1
    );
}

#===================================
sub admin {
#===================================
    my ( $self, $params ) = _params(@_);
    require ElasticSearchX::Autocomplete::GeoNames::Admin;
    ElasticSearchX::Autocomplete::GeoNames::Admin->new(
        geonames => $self,
        debug    => $self->debug,
        %$params,
    );
}

=head1 NAME

ElasticSearchX::Autocomplete::GeoNames - Autocomplete of geolocation data from GeoNames

=head1 VERSION

Version 0.05 - alpha

=head1 DESCRIPTION

C<ElasticSearchX::Autocomplete::GeoNames> provides country/region/city/town/village
autocompete suggestions by building autocomplete indexes from
GeoNames data (see L<http://www.geonames.org/>).

This is an alpha module, completely lacking docs and tests at the moment.

Here be dragons

=head1 SEE ALSO

L<ElasticSearchX::Autocomplete>, L<ElasticSearch>,
L<http://www.elasticsearch.org>

=head1 TODO

=head1 BUGS

If you have any suggestions for improvements, or find any bugs, please report
them to L<https://github.com/clintongormley/ElasticSearchX-Autocomplete-GeoNames/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 AUTHOR

Clinton Gormley, E<lt>clinton@traveljury.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Clinton Gormley

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut

1
