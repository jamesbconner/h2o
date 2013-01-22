import subprocess
import gzip, shutil, random, time, re

# since we hang if hosts has bad IP addresses, thought it'd be nice
# to have simple obvious feedback to user if he's running with -v 
# and machines are down or his hosts definition has bad IPs.
# FIX! currently not used
def ping_host_if_verbose(host):
    # if (h2o.verbose) 
    ping = subprocess.Popen( ["ping", "-c", "4", host]) 
    ping.communicate()

# gunzip gzfile to outfile
def file_gunzip(gzfile, outfile):
    print "\nGunzip-ing", gzfile, "to", outfile
    start = time.time()
    zipped_file = gzip.open(gzfile, 'rb')
    out_file = open(outfile, 'wb')
    out_file.writelines(zipped_file)
    out_file.close()
    zipped_file.close()
    print "\nGunzip took",  (time.time() - start), "secs"

# cat file1 and file2 to outfile
def file_cat(file1, file2, outfile):
    print "\nCat'ing", file1, file2, "to", outfile
    start = time.time()
    destination = open(outfile,'wb')
    shutil.copyfileobj(open(file1,'rb'), destination)
    shutil.copyfileobj(open(file2,'rb'), destination)
    destination.close()
    print "\nCat took",  (time.time() - start), "secs"


def file_shuffle(infile, outfile):
    print "\nShuffle'ing", infile, "to", outfile
    start = time.time()
#    lines = open(infile).readlines()
#    random.shuffle(lines)
#    open(outfile, 'w').writelines(lines)
    fi = open(infile, 'r')
    fo = open(outfile, 'w')
    subprocess.call(["sort", "-R"],stdin=fi, stdout=fo)
    print "\nShuffle took",  (time.time() - start), "secs"


# FIX! This is a hack to deal with parser bug
def file_strip_trailing_spaces(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            # remove various lineends and whitespace (leading and trailing)
            # make it unix linend
            outfile.write(line.strip(" \n\r") + "\n")
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " stripped to " + csvPathname2

# can R deal with comments in a csv?
def file_strip_comments(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            if not line.startswith('#'): outfile.write(line)
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " w/o comments to " + csvPathname2

def file_spaces_to_comma(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            line = re.sub(r' +',r',',line)
            outfile.write(line)
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " with space(s)->comma to " + csvPathname2
