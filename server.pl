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
	$date = $date ? "-d $date" : '';
	my $show_late = $cgi->param('show_late');
	$show_late = $show_late ? '-l' : '';
	my $search = $cgi->param('search');
	$search = $search ? "-s $search" : '';
	my $search_type = $cgi->param('search_type');
	$search_type = $search_type ? "-t $search_type" : '';
	
	my $logstring = FeedForecast::currtime() . sprintf("\thandling request from %s for date %s\n",
		$cgi->remote_addr, $date ? $date : '<auto:now>');
	open LOG, '>>', $log;
	print LOG $logstring;
	close LOG;

	$self->serve_static($cgi, './web');
	print "HTTP/1.0 200 OK\r\n";
	print "Content-type: text/html\n\n";
	print `perl web/index.pl $date $search_type $search $show_late`;
}

sub print_banner {
	my $self = shift;
	print "FeedForecast web application started successfully at http://localhost:" . $self->port . "/\n";
}
