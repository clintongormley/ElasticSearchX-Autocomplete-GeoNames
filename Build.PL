use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name       => 'ElasticSearchX::Autocomplete::GeoNames',
    license           => 'perl',
    dist_author       => q{Clinton Gormley <drtech@cpan.org>},
    dist_version_from => 'lib/ElasticSearchX/Autocomplete/GeoNames.pm',
    build_requires    => {
        'Test::More'      => 0,
        'Test::Exception' => 0,
    },
    add_to_cleanup     => ['ElasticSearchX-Autocomplete-GeoNames*'],
    create_makefile_pl => 'traditional',
    requires           => {
        'ElasticSearchX::Autocomplete' => '0.05',
        'Geo::Distance'                => '0.16',
    },
    recommends => { 'Geo::Distance::XS' => '0.08' },
    meta_merge => {
        resources => {
            repository =>
                'https://github.com/clintongormley/ElasticSearchX-Autocomplete-GeoNames/',
            bugtracker =>
                "https://github.com/clintongormley/ElasticSearchX-Autocomplete-GeoNames/issues",
        }
    },
);

$builder->create_build_script();
