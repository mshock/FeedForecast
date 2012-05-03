#!/usr/bin/perl

# bundled server for hosting portable NN webapp

package WebServer;

use strict;
use Date::Manip qw(ParseDate UnixDate);
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
	
	# write request to server log
	write_log($cgi->remote_addr, $cgi->request_uri);
	
	# static serve web directory for css, charts (later, ajax)
	if ($cgi->path_info =~ m/\.(css|xls|js|ico)/) {
		$self->serve_static($cgi, './web');
		return;
	}
	
	# otherwise serve the perl script output
	
	# get all request params, create switches for calling index.pl
	my $date = $cgi->param('date') ? '-d ' . parse_date($cgi->param('date')) : '';
	my $show_late = $cgi->param('show_late') ? '-l' : '';
	my $search = $cgi->param('search') ? sprintf("-s \"%s\"",$cgi->param('search')) : '';
	my $search_type = $cgi->param('search_type') ? '-t ' . $cgi->param('search_type') : '';
	my $sort = $cgi->param('sort') ? sprintf("-o \"%s\"" , $cgi->param('sort')) : '';

	my $args = "$date $search_type $search $show_late $sort";
	
	print "HTTP/1.0 200 OK\r\n";
	print "Content-type: text/html\n\n";
	print `perl web/index.pl $args`;
}

sub write_log {
	my ($remote_addr, $uri) = @_;
	
	my $logstring = FeedForecast::currtime() . sprintf("\thandling request from %s : %s\n",
		$remote_addr, $uri);
	open LOG, '>>', $log;
	print LOG $logstring;
	close LOG;
}

# overload with custom banner
sub print_banner {
	my $self = shift;
	print "FeedForecast web application started successfully at http://localhost:" . $self->port . "/\n";
}

# turn all varieties of user input into a julian date
sub parse_date {
	my ($input_date) = @_;
	$input_date = ParseDate($input_date);
	return UnixDate($input_date, "%Y%m%d");
}
