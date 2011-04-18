package ElasticSearchX::Autocomplete::GeoNames;

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use ElasticSearchX::Autocomplete;
use base qw(ElasticSearchX::Autocomplete);

#===================================
sub _search_params {
#===================================
    my $self   = shift;
    my $phrase = shift;
    my $params = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $params->{context} = delete $params->{lang};
    return $self->SUPER::_search_params($phrase,$params);
}

our $as_json;
#===================================
sub get_place {
#===================================
    my $self = shift;
    my %params = ref $_[0] ? %{ shift() } : @_;
    $params{context} = $self->clean_context( delete $params{lang} );

    $params{index} = $self->index;
    $params{type}  = $self->type;

    return $self->_try_cache( '_get_place', \%params );
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

    $params->{size} = 1;
    $params->{fields} = [ 'place_id', 'parent_ids', 'location', 'rank' ];

    my $context    = $params->{context};
    my $rank_field = "rank.$context";

    my @filters = { exists => { field => $rank_field } };

    if ( my $label = $params->{label} ) {
        push @filters, { term => { label => $label } };
    }
    else {
        my $place_id = $params->{id};
        return undef unless $place_id;
        push @filters, { term => { place_id => $place_id } };
    }

    my $result = $self->_context_search(
        $params,
        {   query => { constant_score => { filter => { and => \@filters } } },
            script_fields =>
                { rank => { script => "doc['$rank_field'].value" } },
        }
    );
    return undef unless @$result;

    my $fields = $result->[0]{fields};
    for ( 'parent_ids', 'tokens' ) {
        my $val = $fields->{$_};
        $fields->{$_}
            = !defined $val ? []
            : !ref $val     ? [$val]
            :                 $val;
    }

    my ( $lat, $lon ) = split /,/, $fields->{location};
    $fields->{location} = { lat => $lat, lon => $lon };

    $context =~ s{/}{}g;
    $fields->{lang} = $context;

    return $fields;
}

#===================================
sub admin {
#===================================
    my $self = shift;
    require ElasticSearchX::Autocomplete::Geonames::Admin;
    ElasticSearchX::Autocomplete::Geonames::Admin->new( auto => $self );
}

=head1 SEE ALSO

=head1 TODO

=head1 BUGS

None known

=head1 AUTHOR

Clinton Gormley, E<lt>clinton@traveljury.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Clinton Gormley

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut

1
