package App::S3::MySQL::Backup;

use Moo;
use Types::Standard qw[Str];

has [qw<username directory>] => (
    is => 'ro',
    isa => Str,
    required => 1,
);

has [qw<verbose password timestamp Prefix Bucket>]=> (
    is => 'ro',
    isa => Str,
);


sub run {
    my $self = shift;

    chdir $self->directory
        or die "Can't chdir to ", $self->directory, ": $!\n";
    my $now = $self->now();
    my %wanted = map { $_ => 1 } $self->times_to_keep($now);
    my @to_delete;
    my $prefix = $self->Prefix;
    for (glob "$prefix.*") {
        my ($ymdh) = /\A\Q$prefix\E\.([0-9]{10})[0-9]{2}\.gz\z/xms or next;
        push @to_delete, $_ if !$wanted{$ymdh};
    }
    $self->note('Generating backup for ', $now->iso8601);
    $self->backup($prefix . $now->strftime('.%Y%m%d%H%M'));
    if (@to_delete) {
        $self->note('Cleaning up ', scalar @to_delete, ' old backups');
        for (@to_delete) {
            unlink $_ or die "Can't delete $_: $!\n";
            if (my $bucket = $self->bucket) {
                $self->note("Deleting $_ from S3");
                system "s3cmd --no-progress del s3://$bucket/$_";
            }
        }
    }
    $self->note('All done!');
}

sub backup {
    my $self = shift;
    my ($output_file) = @_;

    $output_file .= '.gz' unless $output_file =~ /\.gz$/;

    my $user = $self->username;
    my @creds = "-u\Q$user\E";
    if (defined $self->password) {
        my $pass = $self->password;
        push @creds, "-p\Q$pass\E";
    }

    system "mysqldump --opt @creds --all-databases | gzip > \Q$output_file\E";
    my $dir = $self->directory;
    if ($? != 0) {
        unlink $output_file
            or die "Can't delete incomplete $dir/$output_file: $!\n";
        die "mysqldump exited unsuccessfully; no backup created\n";
    }

    if ($self->Bucket && -e $output_file) {
        system "s3cmd --no-progress put $output_file s3://" . $self->Bucket . "/";
    }
}

sub now {
    my $self = shift;

    return DateTime->now if !$self->timestamp;
    my $rx = join '[^0-9]', ('([0-9]+)') x 6;
    my ($y, $m, $d, $h, $i, $s) = $self->timestamp =~ /\A$rx\z/xms
        or die "Can't parse date/time\n";
    return DateTime->new(
        year => $y, month  => $m, day    => $d,
        hour => $h, minute => $i, second => $s,
    );
}

sub times_to_keep {
    my $self = shift;
    my ($now) = @_;

    my @times;
    my $dt = $now->clone->add(hours => 1);
    for (1 .. 24) {
        $dt->subtract(hours => 1);
        push @times, $dt->clone;
    }
    if ($dt->hour == 0) {
        $dt->subtract(days => 1);
    }
    else {
        $dt->set_hour(0);
    }
    for (1 .. 7) {
        push @times, $dt->clone;
        $dt->subtract(days => 1);
    }
    return map { $_->strftime('%Y%m%d%H') } reverse @times;
}

sub note {
    my $self = shift;
    say STDERR @_ if $self->verbose;
}

1;
