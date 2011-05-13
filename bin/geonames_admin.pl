#!/usr/local/bin/perl

use strict;
use warnings;

use lib 'lib';
use lib '/opt/apache/sites/Projects/ElasticSearch/lib';
use ElasticSearchX::Autocomplete::GeoNames;
use ElasticSearch;

my %Modes = (
    altnames        => \&altnames,
    load            => \&load,
    optimize        => \&optimize,
    delete          => \&delete,
    delete_type     => \&delete_type,
    delete_altnames => \&delete_altnames,
);

## INIT ##

my ( $handler, $opts ) = parse_opts(@ARGV);

our $es = ElasticSearch->new( servers => $opts->{es} || '127.0.0.1:9200' );
our $geonames = ElasticSearchX::Autocomplete::GeoNames->new(
    es    => $es,
    index => $opts->{index} || 'geonames',
    type  => $opts->{type} || 'place',
    debug => 2,
);

our $admin = $geonames->admin( langs => $opts->{langs} || ['en'], );

$handler->();

#===================================
sub altnames {
#===================================
    $admin->index_altnames( @{ $opts->{filenames} } );
}

#===================================
sub load {
#===================================
    $admin->index_places( @{ $opts->{filenames} } );
}

#===================================
sub optimize {
#===================================
    $admin->deploy();
}

#===================================
sub delete {
#===================================
    $admin->delete();
}

#===================================
sub delete_altnames {
#===================================
    $admin->delete_altnames_index();
}

#===================================
sub parse_opts {
#===================================
    my $mode = shift @ARGV
        or die usage();

    my $handler = $Modes{$mode}
        or die usage( $mode eq 'help' ? '' : "Unknown mode '$mode'" );

    my %opts;
    while ( my $key = shift @ARGV ) {
        if ( $key =~ s/^--// ) {
            my $val = shift @ARGV
                or die usage("Missing value for arg --$key");
            $opts{$key} = $val;
        }
        else {
            unshift @ARGV, $key;
            last;
        }
    }
    $opts{filenames} = [@ARGV];
    $opts{langs} = [ split /,/, $opts{langs} ]
        if $opts{langs};
    return ( $handler, \%opts );
}

#===================================
sub usage {
#===================================
    my $error = shift || '';
    if ($error) {
        $error = "\n    ERROR: $error\n";
    }
    return <<USAGE
    $error
    USAGE:
    -----------------------------------------------
    Index alternate names:
      $0 altnames alternateNames.txt myNames.txt

    Index countries:
      $0 load --langs en,fr,de  GB.txt FR.txt

    Optimize index:
      $0 optimize

    Delete index and type:
      $0 delete

    Delete just the type:
      $0 delete_type

    Delete the altername names index:
      $0 delete_altnames

    This help message:
      $0 help

    Optional parameters:
      --index           IndexName               eg   geonames
      --type            TypeName                eg   place
      --alt_index       AltnamesIndex           eg   altnames
      --es              ElasticSearch instance  eg   localhost:9200

USAGE

}

