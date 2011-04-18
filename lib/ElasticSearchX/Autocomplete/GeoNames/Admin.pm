package ElasticSearchX::Autocomplete::GeoNames::Admin;

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use base qw(ElasticSearchX::Autocomplete::Admin);

#===================================
sub type_defn {
#===================================
    my $self = shift;
    my $dfn  = $self->SUPER::type_defn;

    $dfn->{properties}{place_id}
        = { type => 'integer', index => 'not_analyzed',store=>'yes' };

    $dfn->{properties}{parents_id}
        = { type => 'integer', index => 'not_analyzed',store=>'yes' };

    $dfn->{properties}{label}{index} = 'not_analyzed';

    return $dfn;
}

=cut

1
