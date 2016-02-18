#!/usr/bin/perl

use JSON::XS; 
use URL::Builder;
use Time::Local;
use LWP::UserAgent;
use Google::Fusion;
use Getopt::Long;
use Data::Dumper;
use Date::Calc qw( Delta_Days );
use Email::MIME;

use strict;
use warnings;

########################## BEGIN SETUP #########################

# set up args
my %args = (
    'start-date'  => 'yesterday',
    'end-date'    => 'yesterday',
    'config-file' => '',
    'custom-q' => '',
);
GetOptions(
    'start-date=s'  => \$args{'start-date'},
    'end-date=s'    => \$args{'end-date'},
    'config-file=s' => \$args{'config-file'},
	'custom-q=s' => \$args{'custom-q'},
);

# process config-file
process_config_file();

# validate_dates
validate_dates();

# constants
my $BASE_URI        = 'https://www.googleapis.com';
my $PATH            = '/youtube/v3/search';
my $API_KEY         = $args{'api-key'};
my $TABLE_NAME      = $args{'table-name'};
my $FUSION_RESPONSE = "fusiontables#sqlresponse";
my $MAX_RESULTS     = '50';
my $PREFIX_Q        = 'Top 10|5 NBA Plays: ';
my $CHANNEL_ID      = 'UCWJ2lWNubArHWmf3FIHbfcQ';
my $PART            = 'snippet';
my $TYPE            = 'video';
my $SECONDS_PER_DAY = 86400;
my %MONTHS          = (
    0  => 'January', 1  => 'February', 2  => 'March',
    3  => 'April',   4  => 'May',      5  => 'June',
    6  => 'July',    7  => 'August',   8  => 'September',
    9  => 'October', 10 => 'November', 11 => 'December',
);
my %DAYS            = (
    1  => '1st',  2  => '2nd',  3  => '3rd',  4  => '4th',
    5  => '5th',  6  => '6th',  7  => '7th',  8  => '8th',
    9  => '9th',  10 => '10th', 11 => '11th', 12 => '12th',
    13 => '13th', 14 => '14th', 15 => '15th', 16 => '16th',
    17 => '17th', 18 => '18th', 19 => '19th', 20 => '20th',
    21 => '21st', 22 => '22nd', 23 => '23rd', 24 => '24th',
    25 => '25th', 26 => '26th', 27 => '27th', 28 => '28th',
    29 => '29th', 30 => '30th', 31 => '31st',
);

########################## END SETUP ###########################




######################## MAIN PROGRAM ##########################

# get delta between today and start and end dates
my ( $delta_days_end_date, $delta_days_start_date ) = get_delta_days();

# find top plays videos from youtube and insert their data into my fusion table
find_and_insert_videos( $delta_days_end_date, $delta_days_start_date );

##################### END MAIN PROGRAM #########################





######################### SUBROUTINES ##########################

# given a start and end date, returns the days between today and the start date
# and today and the end_date
sub get_delta_days {
    my $start_date = $args{'start-date'};
    my $end_date   = $args{'end-date'};

    # get today's date
    my ( $mday, $mon, $year ) = (localtime())[3 .. 5];
    $year                     = $year + 1900;
    my $today                 = sprintf ( "%02d-%02d-$year", $mon + 1, $mday );

    # if start_date is 'yesterday' then get yesterday's date
    if ( $start_date eq 'yesterday' ) {
        ( $mday, $mon, $year )    = (localtime())[3 .. 5];
        $year                     = $year + 1900;
        my $yesterday             = timelocal(0,0,12,$mday,$mon,$year) - $SECONDS_PER_DAY;
        ( $mday, $mon, $year )    = (localtime($yesterday))[3 .. 5];
        $year                     = $year + 1900;
        $start_date               = sprintf ( "%02d-%02d-$year", $mon + 1, $mday );
        $end_date                 = $start_date;
    }

    # split today's date into parts
    my ( $month_today, $day_today, $year_today ) = split ('-', $today);

    # get the delta days between today and the start_date
    my ( $month1, $day1, $year1 )                = split ('-', $start_date);
    my $delta_days_start_date = Delta_Days(
        $year1, $month1, $day1,
        $year_today, $month_today, $day_today
    );

    # get the delta days between today and the end_date
    my ( $month2, $day2, $year2 )                = split ('-', $end_date);
    my $delta_days_end_date = Delta_Days(
        $year2, $month2, $day2,
        $year_today, $month_today, $day_today
    );

    # verify the start date >= the end date
    die ( "start-date must be >= than end-date" ) if $delta_days_end_date > $delta_days_start_date;

    return ( $delta_days_end_date, $delta_days_start_date );
}

# find top plays videos from youtube and insert their data into my fusion table
# param $delta_days_end_date: number of days between today and end_date
# param $delta_days_start_date: number of days between today and start_date
sub find_and_insert_videos {
    my ( $delta_days_end_date, $delta_days_start_date ) = @_;

    # loop thru each number in reverse order to find the youtube video for that day
    # ex. if $num_days = 5 and today is 02/10/2016, get_dates will get date for 02/05/2016
    foreach my $num_days ( reverse ( $delta_days_end_date .. $delta_days_start_date ) ) {
        # get the dates
        my ( $q, $published_after, $fusion_table_date ) = get_dates( $num_days );

        # set query string params
        $q = $PREFIX_Q . $q;
        
        if ( $args{'custom-q'} ) {
		      $q = $args{'custom-q'};        
        }
        my %query_params = (
            part           => $PART,
            type           => $TYPE,
            channelId      => $CHANNEL_ID,
            key            => $API_KEY,
            maxResults     => $MAX_RESULTS,
            q              => $q,
            publishedAfter => $published_after,
        );

        # build api request
        my $api_request = build_url(
            base_uri => $BASE_URI,
            path     => $PATH,
            query    => \%query_params,
        );

        my $youtube_data = execute_youtube_api_request( $api_request );
        process_youtube_data( $youtube_data, $q, $fusion_table_date );
    }
}

# gets all the needed dates for the API Requests in their needed format
# $q: The "q" query string param value for the youtube video search (i.e. February 7th)
# $published_after: the "publishedAfter" query string param value for the youtubr search (i.e. 2016-02-07T00:00:00)
# $fusion_table_date: the value for the date column in a fusion table insert stmt (i.e. 02/07/2016)
sub get_dates {
    my $num_days                  = shift;
    my ( $mday, $mon, $year ) = (localtime())[3 .. 5];
    my $date                  = timelocal(0,0,12,$mday,$mon,$year) - ($SECONDS_PER_DAY * $num_days );
    ( $mday, $mon, $year )    = (localtime($date))[3 .. 5];
    $year                     = $year + 1900;
    my $q                     = "$MONTHS{$mon} $DAYS{$mday}";
    my $published_after       = sprintf ( "$year-%02d-%02dT00:00:00Z", $mon + 1, $mday );
    my $fusion_table_date     = sprintf ( "%02d/%02d/%04d", $mon + 1, $mday, $year );
    return ( $q, $published_after, $fusion_table_date );
}

# executes youtube API request to get youtube data using 'wget' system command
# returns a perl hash converted from Json
sub execute_youtube_api_request {
    my $api_request = shift;
    my $cmd = "wget -O - '$api_request'";
    my $output = `$cmd`;
    my $data = decode_json( $output );
    return $data;
}

# processes the json from the youtube API request
sub process_youtube_data {
    my ( $data, $q, $fusion_table_date ) = @_;

    my %successful_find;
    my %unsuccessful_find = (
        y_id    => '',
        date    => $fusion_table_date,
        y_title => '',
        status  => 'not found',
        type    => 'default',
    );
    if ( $data ) {
        # the video data is in the items arrary
        my $items = $data->{items};
        if ( scalar @{$items} ) {
            # loop thru each item/video looking for the right video
            foreach my $item ( @{$items} ) {
                my $title       = $item->{snippet}->{title};
                
                # sometimes the video title doesn't have the ('th','rd','st') at the end of the date
                # so, if that's the case, make an alt title to also compare against
                my $alt_title   = "";
                if ( $title =~ m/.*?([0-9]+)$/ ) {
                    my $num = $1;
                    $alt_title = $title;
                    $alt_title =~ s/$num//;
                    $alt_title = $alt_title . $DAYS{$num};
                }

                # sometimes instead of a Top 10, the NBA will release a Top 5 plays instead.
                # so create yet another alt title to also compare against
                my $secondary_q = $q;
                $secondary_q    =~ s/Top 10/Top 5/g;

                # check if this video is the one we're looking for by comparing the title with all
                # our search terms
                if ( $title && ( $title eq $q || $alt_title eq $q || $title eq $secondary_q ) ) {
                    $successful_find{y_id}    = $item->{id}->{videoId};
                    $successful_find{date}    = $fusion_table_date;
                    $successful_find{y_title} = $title;
                    $successful_find{status}  = 'found';
                    $successful_find{type}    = 'default';
                    last;
                }
            }
        }
    }

            # if we got a successful find, insert the video into my fusion table
            if ( %successful_find ) {
                # build the sql
                my $sql = build_sql( \%successful_find );
                print "\n\nSQL:\n$sql\n\n";

                # make API Call to insert/update data into fusion table
                my $json_response = execute_fusion_sql( $sql );
                print "\n\nJSON Response:\n$json_response\n\n";

                # sleep so we don't make too many api calls to Google in a short amount of time
                sleep (2);
            } 
            # if we didn't find the video, insert data saying we didn't find it and send email
            else {
                my $sql = build_sql( \%unsuccessful_find );
                print "\n\nSQL:\n$sql\n\n";

                # make API Call to insert data into fusion table
                my $json_response = execute_fusion_sql( $sql );
                print "\n\nJSON Response:\n$json_response\n\n";

                # sleep so we don't make too many api calls to Google in a short amount of time
                sleep (2);

                # send alert saying video was not found.
                send_email( $fusion_table_date );
            }    
    
}

# build sql statement using the successful find hash
sub build_sql {
    my $sql_parts  = shift;

    # return value
    my $sql = "";

    # first select the data to check if we need to update or insert
    my $select = "SELECT date, rowid FROM $TABLE_NAME WHERE date = '" . $sql_parts->{date} . "'";
    my $select_json_response = execute_fusion_sql( $select );
    my $insert_or_update = "";
    my $row_id;
    if ( $select_json_response ) {
        my $hash = decode_json( $select_json_response );
        if ( $hash->{kind} eq $FUSION_RESPONSE ) {
            if ( $hash->{rows} ) {
                my @rows    = @{$hash->{rows}};
                my $row     = $rows[0];
                $row_id     = $row->[1];
            }
        }

    }

    # if there's already a row in the table for the date then update this data 
    if ( $row_id ) {
        my $prefix_sql = "UPDATE $TABLE_NAME SET ";
        my $ending_sql = " WHERE rowid = '$row_id'";
        my $values     = "";
        foreach my $col ( sort keys %{$sql_parts} ) {
            $values .= "$col = '$sql_parts->{$col}',"
        }
        chop( $values );
        $sql = "$prefix_sql$values$ending_sql";
    }
    # else insert new row
    else {
        my $columns    = join (',', sort keys %{$sql_parts} );
        my $prefix_sql = "INSERT INTO $TABLE_NAME ($columns) VALUES";
        my $values     = "";
        foreach my $col ( sort keys %{$sql_parts} ) {
            $values .= "'$sql_parts->{$col}',"
        }
        chop( $values );
        $sql = "$prefix_sql ($values)";
    }

    return $sql;
}

# Make API call to insert data into Fusion Table
sub execute_fusion_sql {
    my $sql = shift;
    my $fusion = Google::Fusion->new( 
        client_id       => $args{'client-id'},
        client_secret   => $args{'client-secret'},
        token_store     => '/tmp/token_store.txt',
    );
    return $fusion->query( $sql );
}

# send mail
sub send_email {
    my $date = shift;

    my $message = "Top Plays for $date Not Found";
    my $cmd     = "echo $message | mail -s \"$message\" $args{email}"; 
    my $out     = `$cmd`;
}

# process config-file
sub process_config_file {
    if ( !$args{'config-file'} ) {
        die ( "please supply a config-file with the --config-file option. "
            . "This file should be a json formatted file with the following "
            . "key/value pairs: client-id, client-secret, table-name, api-key and email" );
    } else {
        local $/ = undef;
        open ( my $fh, "<", $args{'config-file'} );
        my $json = <$fh>;
        close $fh;
        my $config = decode_json( $json );
        foreach my $config_item ( keys %{$config} ) {
            $args{$config_item} = $config->{$config_item};
        }
    }

    # make sure all required configs were given in the config-file
    foreach ( qw ( client-id client-secret table-name api-key email ) ) {
        if ( !$args{$_} ){
            die ( "Required value in config-file not defined: $_\n" );
        }
        print "\nOption " . $_ . ": " . $args{$_} . "\n";
    }
    print "\n\n";
}


# validate_dates
sub validate_dates {
    # start-date and end-date must be in MM-DD-YYYY format or 'yesterday'
    if ( $args{'start-date'} eq 'yesterday' ) {
        return 1;
    } else {
        my $is_vaild = ( $args{'start-date'} =~ /\d\d-\d\d-\d\d\d\d/ );
        die ( "start-date format is invalid, must be MM-DD-YYYY" ) if !$is_vaild;
        $is_vaild = ( $args{'end-date'} =~ /\d\d-\d\d-\d\d\d\d/ );
        die ( "end-date format is invalid, must be MM-DD-YYYY" ) if !$is_vaild;
    }
}
