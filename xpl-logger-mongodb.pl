#!/usr/bin/perl -w

use xPL::Client;

use MongoDB;
use strict;
use warnings;
use Data::Dumper;

$|=1; # autoflush helps debugging

my $connection = MongoDB::Connection->new(host => 'localhost', port => 27017);
my $database   = $connection->xpl;
my $collection = $database->msg;

my %args = ( vendor_id => 'bnz', device_id => 'listener', );

my $xpl = xPL::Client->new(%args) or die "Failed to create xPL::Client\n";

$xpl->add_xpl_callback(
	id => 'logger',
	self_skip => 0, targetted => 0,
	callback => \&logger,
	filter => "",
);


$xpl->main_loop();

sub logger {
	my %p = @_;
	my $msg = $p{message};
	#print Dumper($msg->body_fields());
	print $msg->interval();
	exit;
	#foreach my $f ($msg->body_fields()) {
	#}
	
	return;
	my $id         = $collection->insert( { 
		class => $msg->class, 
		class_type => $msg->class_type, 
		message_type => $msg->message_type,
		source => $msg->source,
		target => $msg->target,
	} );
};

# send a "hbeat.end" message on exit
END { defined $xpl && $xpl->send_hbeat_end(); }

=head1 AUTHOR

Christophe Nowicki, E<lt>cscm@csquad.orgE<gt>

=head1 COPYRIGHT

Copyright (C) 2005, 2008 by Mark Hindess

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.

=cut
