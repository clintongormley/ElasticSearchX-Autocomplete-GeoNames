#!perl

use Test::More tests => 1;

BEGIN {
    use_ok( 'ElasticSearchX::Autocomplete::GeoNames' ) || print "Bail out!
";
}

diag( "Testing ElasticSearchX::Autocomplete::GeoNames $ElasticSearchX::Autocomplete::GeoNames::VERSION, Perl $], $^X" );
