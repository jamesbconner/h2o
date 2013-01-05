import subprocess
import gzip, shutil, random, time


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



