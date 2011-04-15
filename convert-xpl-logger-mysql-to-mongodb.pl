#!/usr/bin/perl -w
# -*- perl -*-

=head1 NAME

convert-xpl-logger-mysql-to-mongodb.pl - Convert xPL MySQL database to MongoDB

=head1 DESCRIPTION

This script convert an xPL Logger MySQL database (msg,elt and msgelt tables) to
a MongoDB xPL MongoDB collection. 

=head1 USAGE

./convert-xpl-logger-mysql-to-mongodb.pl

=head1 REQUIRED ARGUMENTS

None

=head1 OPTIONS

None

=head1 DIAGNOSTICS

None

=head1 EXIT STATUS

None

=head1 CONFIGURATION

None

=head1 DEPENDENCIES

MongoDB
DateTime
Time::HiRes
DBI
IO::Handle
Readonly
Getopt::Long

=head1 INCOMPATIBILITIES

None

=head1 BUGS AND LIMITATIONS


=head1 AUTHOR

Christophe 'CSCMEU' Nowicki <cscm at csquad dot org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2011 Christophe Nowicki.  All rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.  The full text of this license can
be found in the LICENSE file included with this module.

=head1 VERSION

Version $Revision$ ($Date$)

=cut

use strict;
use warnings;
use English qw(-no_match_vars);

use MongoDB;
use DateTime;
use DBI;
use IO::Handle;
use Readonly;
use Getopt::Long;

use Time::HiRes qw(gettimeofday tv_interval);

Readonly::Scalar my $batch_size => 1000;
our $VERSION = 0.01;


autoflush STDERR, 1;

my $in_host     = 'localhost';
my $in_db       = 'xpl';
my $in_user     = 'root';
my $in_password = 'me';

my $out_host       = 'localhost';
my $out_db         = 'xpl';
my $out_collection = 'msg';

my $usage = 0;

GetOptions(
    "in_host=s"        => \$in_host,
    "in_db=s"          => \$in_db,
    "in_user=s"        => \$in_user,
    "in_password=s"    => \$in_password,
    "out_host=s"       => \$out_host,
    "out_db=s"         => \$out_db,
    "out_collection=s" => \$out_collection,
    "usage|help"       => \$usage,
);

if ($usage) {
    print <<'EOF';
Usage:
      convert-xpl-logger-mysql-to-mongodb.pl [options]
      where valid options are (default shown in brackets):
        -usage - show this help text
        -in_host - input hostname (localhost)
    	-in_db - input database name (xpl)
    	-in_user - input username (root)
    	-in_password - input password (me)
    	-out_host - output hostname (localhost)
    	-out_db - output database name (xpl)
    	-out_collection - output collection (msg)

EOF
    exit 0;
}

my $dbh = DBI->connect( "DBI:mysql:database=$in_db:host=$in_host",
    $in_user, $in_password )
  or die "Can't connect to database: $DBI::errstr\n";

my $c = MongoDB::Connection->new( host => $out_host )
  or die("Could not connect on the MongoDB at $out_host. : $ERRNO\n");

my $db  = $c->$out_db;
my $msg = $db->$out_collection;

#prepare the query
my $sth_msg_count = $dbh->prepare("SELECT COUNT(id) AS count FROM msg");
my $sth_elt_count = $dbh->prepare("SELECT COUNT(id) AS count FROM elt");
my $sth_elt       = $dbh->prepare("SELECT id,name,value FROM elt");

my $sth = $dbh->prepare(
    "SELECT id,time,usec,type,source,target,class FROM msg WHERE id > ? LIMIT ?"
);

my $sth_msgelt = $dbh->prepare("SELECT elt FROM msgelt WHERE msg=?");

# count
$sth_msg_count->execute();
my $msg_count = $sth_msg_count->fetchrow_hashref();

print "Found ", $msg_count->{'count'}, " xPL messages...\n";

$sth_elt_count->execute();
my $elt_count = $sth_elt_count->fetchrow_hashref();

print "Found ", $elt_count->{'count'}, " xPL messages elements ...\n";

print "Loading xPL messages elements ...";

$sth_elt->execute();

my $counter = 0;

my %elt;

while ( my $array_ref = $sth_elt->fetchrow_hashref() ) {
    $elt{ $array_ref->{id} } = {
        name  => $array_ref->{name},
        value => $array_ref->{value}
    };
}

print " done\n";

$counter = 0;
my $start = [gettimeofday];

while ( $counter < $msg_count->{'count'} ) {

    #execute the query
    $sth->execute( $counter, $batch_size );
    my $batch = [];
    while ( my $array_ref = $sth->fetchrow_hashref() ) {
        $sth_msgelt->execute( $array_ref->{id} );
        my $msgelt_ref = $sth_msgelt->fetchall_arrayref();
        foreach my $msgelt ( @{$msgelt_ref} ) {
            my $e = $elt{ $msgelt->[0] };
            $array_ref->{ $e->{'name'} } = $e->{'value'};
        }

        my $id_value =
          sprintf( "%x0000000000%.6x", $array_ref->{time}, $counter, );

        my $id = MongoDB::OID->new( value => $id_value );
        $array_ref->{_id} = $id;
        delete $array_ref->{usec};
        delete $array_ref->{id};
        delete $array_ref->{incomplete};
        delete $array_ref->{time};
        push( @{$batch}, $array_ref );
        $counter++;
    }
    warn "Problem in retrieving results", $sth->errstr(), "\n"
      if $sth->err();

    $msg->batch_insert($batch);
    my $err = $db->last_error( { w => 2 } );

    if ( defined $err->{'err'} ) {
        warn "$err->{'err'}\n";
    }

    my $now     = [gettimeofday];
    my $elapsed = tv_interval( $start, $now );
    my $ratio   = ( $counter / $msg_count->{'count'} );
    printf STDERR "Converting [ %d / %d ] ~ %.3f%% Elapsed %5ds ETA: %5ds.\r",
      $counter, $msg_count->{'count'}, (
        ( $counter / $msg_count->{'count'} ) *
          100,    ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
        $elapsed, $elapsed * ( ( 1 - $ratio ) / ($ratio) )
      );
}

print STDERR "\n";

$sth_msg_count->finish();
$sth_elt_count->finish();
$sth_msgelt->finish();
$sth_elt->finish();
$sth->finish();
$dbh->disconnect or warn "Disconnection error: $DBI::errstr\n";

print "DONE ! Have a nice MapReduce day ;-)\n"

  # $Revision:$
  # $Source:$
