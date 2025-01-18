use strict;
use warnings;
use DBI;
use Text::CSV;
use POSIX qw(strftime);

# Connect to SQL
my $dsn = "DBI:MariaDB:database=avaproject;host=localhost";
my $username = "root";
my $password = "1313";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    PrintError => 0,
    AutoCommit => 1,
}) or die "Unable to connect to database: $DBI::errstr";

# Save and Read CSV
sub read_csv_to_db {
    my ($file_csv) = @_;
    my $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
    
    open my $fh, "<:encoding(utf8)", $file_csv or die "Cannot open $file_csv: $!";
    my $header = $csv->getline($fh);
    
    my $insert_query = "INSERT INTO calls (call_date, caller, called, talk_time, call_status) VALUES (?, ?, ?, ?, ?)";
    my $sth = $dbh->prepare($insert_query);

    while (my $row = $csv->getline($fh)) {
        my ($call_date, $caller, $called, $talk_time, $call_state) = @$row;
        $talk_time = ($talk_time =~ /^\d+$/) ? $talk_time : 0;
        my ($date, $time) = split(/ /, $call_date);
        $call_date = "$date $time";
        $sth->execute($call_date, $caller, $called, $talk_time, $call_state);
    }
    close $fh;
    print "Data successfully inserted to DB.\n";
}

# First Report
sub report_call_status {
    my ($output_file) = @_;
    my $query = "SELECT call_status, COUNT(*) AS count FROM calls GROUP BY call_status";
    my $sth = $dbh->prepare($query);
    $sth->execute();

    open my $fh, ">:encoding(utf8)", $output_file or die "Cannot open $output_file: $!";
    my $csv = Text::CSV->new({ binary => 1, eol => "\n" });
    $csv->print($fh, ["Call status", "Count"]);
    
    while (my $row = $sth->fetchrow_hashref()) {
        $csv->print($fh, [$row->{call_status}, $row->{count}]);
    }
    close $fh;
    print "Report of call status saved in '$output_file'.\n";
}

# Second Report
sub report_long_calls {
    my ($output_file) = @_;
    my $query = "SELECT call_date, caller, called, talk_time FROM calls WHERE talk_time > 300 ORDER BY talk_time DESC";
    my $sth = $dbh->prepare($query);
    $sth->execute();

    open my $fh, ">:encoding(utf8)", $output_file or die "Cannot open $output_file: $!";
    my $csv = Text::CSV->new({ binary => 1, eol => "\n" });
    $csv->print($fh, ["Call date", "Caller", "Called", "Talk time"]);
    
    while (my $row = $sth->fetchrow_hashref()) {
        $csv->print($fh, [$row->{call_date}, $row->{caller}, $row->{called}, $row->{talk_time}]);
    }
    close $fh;
    print "Report of long calls saved in '$output_file'.\n";
}

# Third Report
sub report_calls_caller {
    my ($output_file) = @_;
    my $query = qq{
        SELECT caller,
            SUM(CASE WHEN call_status = 'Answered' THEN 1 ELSE 0 END) AS Answered,
            SUM(CASE WHEN call_status = 'Busy' THEN 1 ELSE 0 END) AS Busy,
            SUM(CASE WHEN call_status = 'Failed' THEN 1 ELSE 0 END) AS Failed,
            SUM(CASE WHEN call_status = 'Not Answered' THEN 1 ELSE 0 END) AS Not_Answered
        FROM calls
        GROUP BY caller
    };
    my $sth = $dbh->prepare($query);
    $sth->execute();

    open my $fh, ">:encoding(utf8)", $output_file or die "Cannot open $output_file: $!";
    my $csv = Text::CSV->new({ binary => 1, eol => "\n" });
    $csv->print($fh, ["Caller", "Answered", "Busy", "Failed", "Not Answered"]);
    
    while (my $row = $sth->fetchrow_hashref()) {
        $csv->print($fh, [$row->{caller}, $row->{Answered}, $row->{Busy}, $row->{Failed}, $row->{Not_Answered}]);
    }
    close $fh;
    print "Report of calls per caller saved in '$output_file'.\n";
}


my $file_csv = "CDR_202412012359.csv";
my $output_file1 = "call_status_report.csv";
my $output_file2 = "long_calls_report.csv";
my $output_file3 = "calls_per_caller_report.csv";
read_csv_to_db($file_csv);
report_call_status($output_file1);
report_long_calls($output_file2);
report_calls_caller($output_file3);


$dbh->disconnect();