#!/usr/bin/perl

# bundled server for hosting portable NN webapp

package WebServer;

use strict;
use base qw(HTTP::Server::Simple::CGI);
use HTTP::Server::Simple::Static;
use FeedForecast;

my $config = FeedForecast::loadConfig();

my $server = WebServer->new($config->serverport());
my $log = $config->serverlog();
my $verbose = $config->serververbose();

$server->run();

sub handle_request {
	my ($self, $cgi) = @_;
	
	my $date = $cgi->param('date');
	
	my $logstring = FeedForecast::currtime() . sprintf("\thandling request from %s for date %s\n",
		$cgi->remote_addr, $date ? $date : '<auto:now>');
	open LOG, '>>', $log;
	print LOG $logstring;
	close LOG;

	$self->serve_static($cgi, './web');
	print "HTTP/1.0 200 OK\r\n";
	print "Content-type: text/html\n\n";
	print `perl web/index.pl "$date"`;
}

sub print_banner {
	my $self = shift;
	print "FeedForecast web application started successfully at http://localhost:" . $self->port . "/\n";
}
