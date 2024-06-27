
# Take input
$fl = <STDIN>;
chomp $fl;
$/ = undef;

open($fin, '<', $fl) or die "Cannot open file\n";
$clingout = <$fin>;
close $fin;

# Find number of solutions and runtime
if ($clingout =~ /Models\s*:\s*(\S*)\+/) { print "$1\n"; }
elsif ($clingout =~ /Models\s*:\s*(\S*)/) { print "$1\n"; }
else { print "-1\n"; }

if ($clingout =~ /Time\s*:\s*(\S*)s/) { print "$1\n"; }
else { print "0\n"; }
