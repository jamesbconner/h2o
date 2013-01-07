import time, os, json, signal, tempfile, shutil, datetime, inspect, threading, os.path, getpass
import requests, psutil, argparse, sys, unittest
import glob
import h2o_browse as h2b
import re
import inspect, webbrowser

# For checking ports in use, using netstat thru a subprocess.
from subprocess import Popen, PIPE

# the cloud is uniquely named per user (only)
# if that's sufficient, it should be fine to uniquely identify the flatfile by name only also
# (both are the user that runs the test. The config might have a different username on the
# remote machine (0xdiag, say, or hduser)
def flatfile_name():
    return('pytest_flatfile-%s' %getpass.getuser())

def cloud_name():
    return('pytest-%s-%s' % (getpass.getuser(), os.getpid()))
    # return('pytest-%s' % getpass.getuser())

def __drain(src, dst):
    for l in src:
        if type(dst) == type(0):
            os.write(dst, l)
        else:
            dst.write(l)
            dst.flush()
    src.close()
    if type(dst) == type(0):
        os.close(dst)

def drain(src, dst):
    t = threading.Thread(target=__drain, args=(src,dst))
    t.daemon = True
    t.start()

def unit_main():
    print "\nRunning: python", inspect.stack()[1][1]
    # moved clean_sandbox out of here, because nosetests doesn't execute h2o.unit_main in our tests.
    parse_our_args()
    unittest.main()

# global disable. used to prevent browsing when running nosetests, or when given -bd arg
# also, if user=jenkins. defaults to True, if h2o.unit_main isn't executed, so parse_our_args isn't executed.
# since nosetests doesn't execute h2o.unit_main, it should have the browser disabled..I think?
browse_disable = True
browse_json = False
verbose = False
ipaddr = None
config_json = False
debugger = False
new_json = False

def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('-bd', '--browse_disable', help="Disable any web browser stuff. Needed for batch. nosetests and jenkins disable browser through other means already, so don't need", action='store_true')
    parser.add_argument('-b', '--browse_json', help='Pops a browser to selected json equivalent urls. Selective. Also keeps test alive (and H2O alive) till you ctrl-c. Then should do clean exit', action='store_true')
    parser.add_argument('-v', '--verbose', help='increased output', action='store_true')
    parser.add_argument('-ip', '--ip', type=str, help='IP address to use for single host H2O with psutil control')
    parser.add_argument('-cj', '--config_json', help='Use this json format file to provide multi-host defaults. Overrides the default file pytest_config-<username>.json. These are used only if you do build_cloud_with_hosts()')
    parser.add_argument('-dbg', '--debugger', help='Launch java processes with java debug attach mechanisms', action='store_true')
    parser.add_argument('-new', '--new_json', help='do all functions through the new API port', action='store_true')
    parser.add_argument('-old', '--old_json', help='do GLM and RF functions through the old HTTP port', action='store_true')
    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    global browse_disable, browse_json, verbose, ipaddr, config_json, debugger, new_json

    browse_disable = args.browse_disable or getpass.getuser()=='jenkins'
    browse_json = args.browse_json
    verbose = args.verbose
    ipaddr = args.ip
    config_json = args.config_json
    debugger = args.debugger
    # defaults to new_json=true if neither new nor old. 
    # old controls if new not present.
    new_json = args.new_json or (not args.old_json)

    # set sys.argv to the unittest args (leav sys.argv[0] as is)
    # FIX! this isn't working to grab the args we don't care about
    # Pass "--failfast" to stop on first error to unittest. and -v
    sys.argv[1:] = ['-v', "--failfast"] + args.unittest_args

def verboseprint(*args, **kwargs):
    if verbose:
        for x in args: # so you don't have to create a single string
            print x,
        for x in kwargs: # so you don't have to create a single string
            print x,
        print
        # so we can see problems when hung?
        sys.stdout.flush()

def find_dataset(f):
    (head, tail) = os.path.split(os.path.abspath('datasets'))
    verboseprint("find_dataset looking upwards from", head, "for", tail)
    # don't spin forever 
    levels = 0
    while not (os.path.exists(os.path.join(head, tail))):
        head = os.path.split(head)[0]
        levels += 1
        if (levels==10): 
            raise Exception("unable to find datasets. Did you git it?")

    return os.path.join(head, tail, f)

def find_file(base):
    f = base
    if not os.path.exists(f): f = '../' + base
    if not os.path.exists(f): f = '../../' + base
    if not os.path.exists(f): f = 'py/' + base
    if not os.path.exists(f):
        raise Exception("unable to find file %s" % base)
    return f

# Return file size.
def get_file_size(f):
    return os.path.getsize(f)

# Splits file into chunks of given size and returns an iterator over chunks.
def iter_chunked_file(file, chunk_size=2048):
    return iter(lambda: file.read(chunk_size), '')

LOG_DIR = 'sandbox'
def clean_sandbox():
    if os.path.exists(LOG_DIR):
        # shutil.rmtree fails to delete very long filenames on Windoze
        #shutil.rmtree(LOG_DIR)
        # This seems reliable on windows+cygwin
        os.system("rm -rf "+LOG_DIR)
    os.mkdir(LOG_DIR)

def clean_sandbox_stdout_stderr():
    if os.path.exists(LOG_DIR):
        files = []
        # glob.glob returns an iterator
        for f in glob.glob(LOG_DIR + '/*stdout*'):
            verboseprint("cleaning", f)
            os.remove(f)
        for f in glob.glob(LOG_DIR + '/*stderr*'):
            verboseprint("cleaning", f)
            os.remove(f)

def tmp_file(prefix='', suffix=''):
    return tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)
def tmp_dir(prefix='', suffix=''):
    return tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)

def log(cmd, comment=None):
    with open(LOG_DIR + '/commands.log', 'a') as f:
        f.write(str(datetime.datetime.now()) + ' -- ')
        f.write(cmd)
        if comment:
            f.write('    #')
            f.write(comment)
        f.write("\n")

def make_syn_dir():
    SYNDATASETS_DIR = './syn_datasets'
    if os.path.exists(SYNDATASETS_DIR):
        shutil.rmtree(SYNDATASETS_DIR)
    os.mkdir(SYNDATASETS_DIR)
    return SYNDATASETS_DIR

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
def get_ip_address():
    if ipaddr:
        verboseprint("get_ip case 1:", ipaddr)
        return ipaddr

    import socket
    ip = '127.0.0.1'
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',0))
        ip = s.getsockname()[0]
        verboseprint("get_ip case 2:", ip)
    except:
        pass

    if ip.startswith('127'):
        ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]
        verboseprint("get_ip case 3:", ip)

    verboseprint("get_ip_address:", ip) 
    return ip

def spawn_cmd(name, args, capture_output=True):
    if capture_output:
        outfd,outpath = tmp_file(name + '.stdout.', '.log')
        errfd,errpath = tmp_file(name + '.stderr.', '.log')
        ps = psutil.Popen(args, stdin=None, stdout=outfd, stderr=errfd)
    else:
        outpath = '<stdout>'
        errpath = '<stderr>'
        ps = psutil.Popen(args)

    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    log(' '.join(args), comment=comment)
    return (ps, outpath, errpath)

def spawn_cmd_and_wait(name, args, timeout=None):
    (ps, stdout, stderr) = spawn_cmd(name, args)

    rc = ps.wait(timeout)
    out = file(stdout).read()
    err = file(stderr).read()

    if rc is None:
        ps.terminate()
        raise Exception("%s %s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                (name, args, timeout or 0, out, err))
    elif rc != 0:
        raise Exception("%s %s failed.\nstdout:\n%s\n\nstderr:\n%s" % (name, args, out, err))

# used to get a browser pointing to the last RFview
global json_url_history
json_url_history = []

global nodes
nodes = []

def write_flatfile(node_count=2, base_port=54321, hosts=None):
    # always create the flatfile. 
    ports_per_node = 3
    pff = open(flatfile_name(), "w+")
    if hosts is None:
        ip = get_ip_address()
        for i in xrange(node_count):
            pff.write("/" + ip + ":" + str(base_port +ports_per_node*i) + "\n")
    else:
        for h in hosts:
            for i in xrange(node_count):
                pff.write("/" + h.addr + ":" + str(base_port +ports_per_node*i) + "\n")
    pff.close()


def check_port_group(baseport):
    # for now, only check for jenkins or kevin
    username = getpass.getuser()
    if username=='jenkins' or username=='kevin' or username=='michal':
        # assumes you want to know about 3 ports starting at baseport
        command1Split = ['netstat', '-anp']
        command2Split = ['egrep']
        # colon so only match ports. space at end? so no submatches
        command2Split.append("(" + str(baseport) + "|" + str(baseport+1) + "|" + str(baseport+2) + ")")
        command3Split = ['wc','-l']

        print "Checking 3 ports starting at ", baseport
        print ' '.join(command2Split)

        # use netstat thru subprocess
        p1 = Popen(command1Split, stdout=PIPE)
        p2 = Popen(command2Split, stdin=p1.stdout, stdout=PIPE)
        p3 = Popen(command3Split, stdin=p2.stdout, stdout=PIPE)
        output = p3.communicate()[0]
        print output

# node_count is per host if hosts is specified.
def build_cloud(node_count=2, base_port=54321, hosts=None, 
        timeoutSecs=20, retryDelaySecs=0.5, cleanup=True, **kwargs):
    # moved to here from unit_main. so will run with nosetests too!
    clean_sandbox()
    ports_per_node = 3 # 3 because we have the API port now too
    node_list = []
    try:
        # if no hosts list, use psutil method on local host.
        totalNodes = 0
        if hosts is None:
            hostCount = 1
            for i in xrange(node_count):
                verboseprint("psutil starting node", i)
                newNode = LocalH2O(port=base_port + i*ports_per_node, node_id=totalNodes, **kwargs)
                node_list.append(newNode)
                totalNodes += 1
        else:
            hostCount = len(hosts)
            for h in hosts:
                for i in xrange(node_count):
                    verboseprint('ssh starting node', i, 'via', h)
                    newNode = h.remote_h2o(port=base_port + i*ports_per_node, node_id=totalNodes, **kwargs)
                    node_list.append(newNode)
                    totalNodes += 1

        verboseprint("Attempting Cloud stabilize of", totalNodes, "nodes on", hostCount, "hosts")
        start = time.time()
        stabilize_cloud(node_list[0], len(node_list), 
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
        verboseprint(len(node_list), " Node 0 stabilized in ", time.time()-start, " secs")
        verboseprint("Built cloud: %d node_list, %d hosts, in %d s" % (len(node_list), 
            hostCount, (time.time() - start))) 

        # FIX! using "consensus" in node[0] should mean this is unnecessary?
        # maybe there's a bug. For now do this. long term: don't want?
        # For now, only do this for remote case.
        if hosts is not None:
            for n in nodes:
                stabilize_cloud(n, len(nodes), timeoutSecs=15)

    except:
        if cleanup:
            for n in node_list: n.terminate()
        else:
            nodes[:] = node_list
        if (check_sandbox_for_errors()):
            raise Exception("Errors in sandbox stdout or stderr." +  
                "Could have occured at any prior time")
        raise

    # this is just in case they don't assign the return to the nodes global?
    nodes[:] = node_list
    return node_list

def upload_jar_to_remote_hosts(hosts, slow_connection=False):
    def prog(sofar, total):
        # output is bad for jenkins. 
        # ok to turn this off for all cases where we don't want a browser
        if not browse_disable:
            p = int(10.0 * sofar / total)
            sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#'*p, ' '*(10-p), 100*sofar/total))
            sys.stdout.flush()
        
    if not slow_connection:
        for h in hosts:
            f = find_file('build/h2o.jar')
            h.upload_file(f, progress=prog)
            # skipping progress indicator for the flatfile
            h.upload_file(flatfile_name())
    else:
        f = find_file('build/h2o.jar')
        hosts[0].upload_file(f, progress=prog)
        hosts[0].push_file_to_remotes(f, hosts[1:])

        f = find_file(flatfile_name())
        hosts[0].upload_file(f, progress=prog)
        hosts[0].push_file_to_remotes(f, hosts[1:])

def check_sandbox_for_errors():
    # Dump any assertion or error line to the screen
    # Both "passing" and failing tests??? I guess that's good.

    # if you find a problem, just keep printing till the end, in that file. 
    # The stdout/stderr is shared for the entire cloud session?
    # so don't want to dump it multiple times?

    # "printing" is per file. foundAnyBadness is about the collection    `   
    foundAnyBadness = False
    for filename in os.listdir(LOG_DIR):
        if re.search('stdout|stderr',filename):
            sandFile = open(LOG_DIR + "/" + filename, "r")
            # just in case rror/ssert is lower or upper case
            # FIX! aren't we going to get the cloud building info failure messages
            # oh well...if so ..it's a bug! "killing" is temp to detect jar mismatch error
            regex1 = re.compile(
                'exception|error|assert|warn|info|killing|killed|required ports',
                re.IGNORECASE)
            regex2 = re.compile('Caused')
            regex3 = re.compile('warn|info', re.IGNORECASE)

            # if we started due to "warning" ...then if we hit exception, we don't want to stop
            # we want that to act like a new beginning. Maybe just treat "warning" and "info" as
            # single line events? that's better

            printing = 0
            lines = 0
            for line in sandFile:
                # no multiline FSM on this 
                printSingleLine = regex3.search(line)
                foundBad = regex1.search(line) and ('error rate' not in line)
                if (printing==0 and foundBad):
                    printing = 1
                    lines = 1
                    foundAnyBadness = True
                elif (printing==1):
                    lines += 1
                    # if we've been printing, stop when you get to another error
                    # keep printing if the pattern match for the condition
                    # is on a line with "Caused" in it ("Caused by")
                    # only use caused for overriding an end condition
                    foundCaused = regex2.search(line)
                    # since the "at ..." lines may have the "bad words" in them, we also don't want 
                    # to stop if a line has " *at " at the beginning.
                    # Update: Assertion can be followed by Exception. 
                    # Make sure we keep printing for a min of 4 lines
                    foundAt = re.match(r'[\t ]+at ',line)
                    if foundBad and (lines>4) and not (foundCaused or foundAt):
                        printing = 2 

                if (printing==1 or printSingleLine):
                    # to avoid extra newline from print. line already has one
                    sys.stdout.write(line)

            sandFile.close()

    return (foundAnyBadness) # can test and cause exception


def tear_down_cloud(node_list=None):
    if not node_list: node_list = nodes
    try:
        for n in node_list:
            n.terminate()
            verboseprint("tear_down_cloud n:", n)
    finally:
        node_list[:] = []
        if (check_sandbox_for_errors()):
            raise Exception("tear_down_cloud: Errors in sandbox stdout or stderr." +
                "Could have occured at any prior time")

# don't need this any more? 
# used to need it to make sure cloud didn't go away between unittest defs
def touch_cloud(node_list=None):
    # Only need to use this if we're using hosts? 
    # So far, we don't need hosts as global ..so don't look at it here
    # won't break if local host, just don't need to call this.
    if not node_list: node_list = nodes
    for n in nodes:
        # verboseprint("Keeping remote H2O channels alive", n)
        n.is_alive()
    
def stabilize_cloud(node, node_count, timeoutSecs=14.0, retryDelaySecs=0.25):
    node.wait_for_node_to_accept_connections(timeoutSecs)
    # want node saying cloud = expected size, plus thinking everyone agrees with that.
    def test(n):
        c = n.get_cloud()
        # don't want to check everything. But this will check that the keys are returned!
        consensus  = c['consensus']
        locked     = c['locked']
        cloud_size = c['cloud_size']
        cloud_name = c['cloud_name']
        node_name  = c['node_name']
        cnodes      = c['nodes'] # list of dicts 

        if (cloud_size > node_count):
            print "\nNodes in current cloud:"
            for c in cnodes:
                print c['name']
        
            emsg = (
                "\n\nERROR: cloud_size: %d reported via json is bigger than we expect: %d" % (cloud_size, node_count) +
                "\nYou likely have zombie(s) with the same cloud name on the network, that's forming up with you." +
                "\nLook at the cloud IP's in 'grep Paxos sandbox/*stdout*' for some IP's you didn't expect." +
                "\n\nYou probably don't have to do anything, as the cloud shutdown in this test should"  +
                "\nhave sent a Shutdown.json to all in that cloud (you'll see a kill -2 in the *stdout*)." +
                "\nIf you try again, and it still fails, go to those IPs and kill the zombie h2o's." +
                "\nIf you think you really have an intermittent cloud build, report it." +
                "\n" +
                "\nUPDATE: building cloud size of 2 with 127.0.0.1 may temporarily report 3 incorrectly, with no zombie?" 
                )
            # raise Exception(emsg)
            print emsg

        
        a = (cloud_size==node_count and consensus)
        if (a):
            verboseprint("\tLocked won't happen until after keys are written")
            verboseprint("\nNodes in current cloud:")
            for c in cnodes:
                verboseprint(c['name'])

        return(a)

    node.stabilize(test, error=('A cloud of size %d' % node_count),
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

class H2O(object):
    def __url(self, loc, port=None, new=False):
        if port is None: port = self.port
        if new: port += 2
        u = 'http://%s:%d/%s' % (self.addr, port, loc)
        return u 

    def __check_request(self, r, extraComment=None):
        if extraComment:
            log('Sent ' + r.url + " # " + extraComment)
        else:
            log('Sent ' + r.url)

        if not r:
            raise Exception('r Error in %s: %s' % (inspect.stack()[1][3], str(r)))
        # this is used to open a browser on RFview results to see confusion matrix
        # we don't' have that may urls flying around, so let's keep them all
        # the browser can walk back until it hits a RFview. I suppose this should be an object.
        json_url_history.append(r.url)

        # HACK: excessive messaging variance ..so check a bunch...should jira this issue
        rjson = r.json
        if 'error' in rjson:
            print rjson
            raise Exception('rjson error in %s: %s' % (inspect.stack()[1][3], rjson['error']))
        elif 'Error' in rjson:
            print rjson
            raise Exception('rjson Error in %s: %s' % (inspect.stack()[1][3], rjson['Error']))
        elif 'warning' in rjson:
            print 'rjson warning in %s: %s' % (inspect.stack()[1][3], rjson['warning'])
        elif 'Warning' in rjson:
            print 'rjson Warning in %s: %s' % (inspect.stack()[1][3], rjson['Warning'])
        elif 'warnings' in rjson:
            print 'rjson warnings in %s: %s' % (inspect.stack()[1][3], rjson['warnings'])
        elif 'Warnings' in rjson:
            print 'rjson Warnings in %s: %s' % (inspect.stack()[1][3], rjson['Warnings'])

        return rjson

    def test_redirect(self):
        return self.__check_request(requests.get(self.__url('TestRedirect.json', new=True)))
    def test_poll(self, args):
        return self.__check_request(requests.get(
                    self.__url('TestPoll.json', new=True),
                    params=args))


    def get_cloud(self):
        a = self.__check_request(requests.get(self.__url('Cloud.json', new=True)))
        # don't want to print everything from get_cloud json (f/j info etc)
        # but this will check the keys exist!
        consensus  = a['consensus']
        locked     = a['locked']
        cloud_size = a['cloud_size']
        cloud_name = a['cloud_name']
        node_name  = a['node_name']
        verboseprint('%s%s %s%s %s%s' %(
            "\tcloud_size: ", cloud_size,
            "\tconsensus: ", consensus,
            "\tlocked: ", locked
            ))
        return a

    def get_timeline(self):
        return self.__check_request(requests.get(self.__url('Timeline.json', new=True)))

    # Shutdown url is like a reset button. Doesn't send a response before it kills stuff
    # safer if random things are wedged, rather than requiring response
    # so request library might retry and get exception. allow that.
    def shutdown_all(self):
        try:
            self.__check_request(requests.get(self.__url('Shutdown.json', new=True)))
        except:
            pass
        return(True)

    def put_value(self, value, key=None, repl=None):
        return self.__check_request(
            requests.get(
                self.__url('PutValue.json', new=True), 
                params={"value": value, "key": key, "replication_factor": repl}),
            extraComment = str(value) + "," + str(key) + "," + str(repl))

    def put_file(self, f, key=None, timeoutSecs=60):
        resp1 =  self.__check_request(
            requests.get(
                self.__url('WWWFileUpload.json', new=True), 
                timeout=timeoutSecs,
                params={"Key": key}), 
            extraComment = str(f) + "," + str(key))

        verboseprint("\nput_file #1 phase response: ", dump_json(resp1))
        resp2 = self.__check_request(
            requests.post(
                self.__url('Upload.json', port=resp1['port']), 
                files={"File": open(f, 'rb')}),
            extraComment = str(f))

        verboseprint("put_file #2 phase response: ", dump_json(resp2))
        return resp2[0]
    
    def get_key(self, key):
        return requests.get(self.__url('Get.html', new=True),
            prefetch=False,
            params={"key": key})

    def poll_url(self, response, timeoutSecs=10, retryDelaySecs=0.2):
        url = self.__url(response['redirect_request'], new=True)
        args = response['redirect_request_args']
        status = 'poll'
        r = None
        start = time.time()
        count = 0
        while status == 'poll':
            if r: time.sleep(retryDelaySecs)
            r = self.__check_request(
                requests.get(
                    url=url, 
                    timeout=7, # polling should never take more than 7 secs to respond
                    params=args))
            if ((count%10)==0):
                verboseprint('Polling with', url, 'Response:', dump_json(r['response']))

            status = r['response']['status']
            if ((time.time()-start)>timeoutSecs):
                # show what we're polling with 
                argsStr =  '&'.join(['%s=%s' % (k,v) for (k,v) in args.items()])
                emsg = "Timeout while polling. status: " + status + " url: " + url + "?" + argsStr
                raise Exception(emsg)
            count += 1
        return r

    def parse(self, key, key2=None, timeoutSecs=300, retryDelaySecs=0.2, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        # this doesn't work. webforums indicate max_retries might be 0 already? (as of 3 months ago)
        # requests.defaults({max_retries : 4})
        # https://github.com/kennethreitz/requests/issues/719
        # it was closed saying Requests doesn't do retries. (documentation implies otherwise)
        verboseprint("\nParsing key:", key, "to key2:", key2, "(if None, means default)")

        a = self.__check_request(
            requests.get(
                url=self.__url('Parse.json', new=True),
                timeout=timeoutSecs,
                params={"source_key": key, "destination_key": key2}))
        # Check that the response has the right ParseProgress url it's going to steer us to.
        if a['response']['redirect_request']!='ParseProgress':
            print dump_json(a)
            raise Exception('H2O parse redirect is not ParseProgress. Parse json response precedes.')
        a = self.poll_url(a['response'], timeoutSecs=timeoutSecs, retryDelaySecs=0.2)
        verboseprint("\nParse result:", dump_json(a))
        return a

    def netstat(self):
        return self.__check_request(requests.get(self.__url('Network.json', new=True)))

    def jstack(self):
        return self.__check_request(requests.get(self.__url("JStack.json", new=True)))

    # &offset=
    # &view=
    def inspect(self, key, offset=None, view=None):
        a = self.__check_request(requests.get(self.__url('Inspect.json', new=True),
            params={
                "key": key,
                "offset": offset,
                "view": view,
                }))
        ### verboseprint("\ninspect result:", dump_json(a))
        return a

    # H2O doesn't support yet?
    def Store2HDFS(self, key):
        a = self.__check_request(requests.get(self.__url('Store2HDFS.json'),
            params={"Key": key}))
        verboseprint("\ninspect result:", dump_json(a))
        return a

    def import_folder(self, folder, repl=None):
        a = self.__check_request(requests.get(
            self.__url('ImportFolder.json'),
            params={
                "Folder": folder,
                "rf": repl}))
        verboseprint("\nimport_folder result:", dump_json(a))
        return a

    def exec_query(self, timeoutSecs=20, **kwargs):
        params_dict = {
            'Expr': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)
        verboseprint("\nexec_query:", params_dict)
        a = self.__check_request(requests.get(
            # FIX! force to old because doesn't exist in new yet
            # url=self.__url('Exec.json', new=new_json),
            url=self.__url('Exec.json', new=False),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\nexec_query result:", dump_json(a))
        return a

    # kwargs used to pass:
    # RF?
    # classWt=&
    # class=2&
    # ntree=&
    # modelKey=& 
    # OOBEE=true&
    # gini=1&
    # depth=&
    # binLimit=&
    # parallel=&
    # ignore=&   ...this is ignore columns
    # sample=&
    # seed=&
    # features=1&
    # singlethreaded=0&     ..debug only..may be gone
    # Key=chess_2x2_500_int.hex

    # note ntree in kwargs can overwrite trees!
    def random_forest(self, key, trees, timeoutSecs=300, **kwargs):
        # print "new_json:", new_json
        # FIX!. this new/old if-else stuff can go away once we transition and migrate the tests to new
        # param names
        if new_json:
            modelKey = kwargs.pop('modelKey', 'pytest_model')
            params_dict = {
                'data_key': key,
                'ntree':  trees,
                'model_key': modelKey,
                'depth': 30,
                }
        else:
            params_dict = {
                'Key': key,
                'ntree': trees,
                'modelKey': 'pytest_model',
                'depth': 30,
                }
        
        browseAlso = kwargs.pop('browseAlso',False)
        clazz = kwargs.pop('clazz', None)
        if clazz is not None: params_dict['class'] = clazz

        params_dict.update(kwargs)
        verboseprint("\nrandom_forest parameters:", params_dict)
        a = self.__check_request(requests.get(
            url=self.__url('RF.json', new=new_json), 
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\nrandom_forest result:", dump_json(a))
        return a

    # kwargs used to pass:
    # RFView?
    # classWt=classWt=B=1,W=2&
    # class=2&
    # ntree=50&
    # modelKey=model&
    # OOBEE=true&
    # singlethreaded=0&     ..debug only..
    # dataKey=chess_2x2_500_int.hex
    # ignore=&   ...this is ignore columns
    def random_forest_view(self, dataKey, modelKey, timeoutSecs=300, **kwargs):
        # UPDATE: only pass the minimal set of params to RFView. It should get the 
        # rest from the model. what about classWt? It can be different between RF and RFView?
        # Will need to update this list if we params for RfView
        # FIX!. new/old if-else stuff can go away once we transition and migrate the tests to new
        # param names
        if new_json:
            params_dict = {
                'data_key': dataKey,
                'model_key': modelKey,
                'OOBEE': None,
                'classWt': None,
                'class': None, # FIX! apparently this is needed now?
                }
        else: 
            params_dict = {
                'dataKey': dataKey,
                'modelKey': modelKey,
                'OOBEE': None,
                'classWt': None,
                'class': None, # FIX! apparently this is needed now?
                }

        browseAlso = kwargs.pop('browseAlso',False)
        clazz = kwargs.pop('clazz', None)
        if clazz is not None: params_dict['class'] = clazz
        # only update params_dict..don't add
        # throw away anything else as it should come from the model (propagating what RF used)
        for k in kwargs:
            if k in params_dict:
                params_dict[k] = kwargs[k]

        a = self.__check_request(requests.get(
            self.__url('RFView.json', new=new_json), 
            timeout=timeoutSecs,
            params=params_dict))

        verboseprint("\nrandom_forest_view result:", dump_json(a))
        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        return a

    def random_forest_treeview(self, n, dataKey, modelKey, timeoutSecs=10, **kwargs):
        if new_json:
            params_dict = {
                'tree_number': n,
                'data_key': dataKey,
                'model_key': modelKey,
                }
        else:
            params_dict = {
                'n': n,
                'dataKey': dataKey,
                'modelKey': modelKey
                }

        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)

        if (1==0):
            a = self.__check_request(requests.get(
                self.__url('RFTreeView.json', new=new_json),
                timeout=timeoutSecs,
                params=params_dict))

            verboseprint("\nrandom_forest_treeview result:", dump_json(a))
            if (browseAlso | browse_json):
                h2b.browseJsonHistoryAsUrlLastMatch("RFTreeView")
        else:
            a = "No RFTreeView.json implemented yet hacking a webbrowser instead"
            print "\n", a
            if new_json:
                url = self.__url('RFTreeView.html', new=True)
            else:
                url = self.__url('RFTreeView', new=False)
            # tack on the params. We're not logging this url
            joiner = "?"
            for k,v in params_dict.iteritems():
                url = url + joiner + k + "=" + str(v)
                joiner = "&"
            webbrowser.open_new(url)
            time.sleep(3) # to be able to see it
        return a

    def linear_reg(self, key, timeoutSecs=10, **kwargs):
        if new_json:
            params_dict = {
                'key': key,
                'colA': 0,
                'colB': 1,
            }
        else:
            params_dict = {
                'Key': key,
                'colA': 0,
                'colB': 1,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)

        a = self.__check_request(requests.get(
            self.__url('LR.json', new=new_json),
            timeout=timeoutSecs,
            params=params_dict))

        verboseprint("\nlinear_reg result:", dump_json(a))
        return a


    # kwargs used to pass many params
    # names are changing (old/new port)
    # new_json is only enabled by "python test_* -new"
    def GLM_shared(self, key, timeoutSecs=300, retryDelaySecs=0.5, parentName=None, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        print "new_json:", new_json
        # FIX!.new/old if-else stuff can go away once we transition and migrate the tests to new
        # param names
        if new_json:
            # if the old 'Y' param is in use, change it to the new 'y' param
            # if 'y' is used, it will override this
            # same with x. key changed also.
            y = kwargs.pop('Y', 1)
            x = kwargs.pop('X', None)
            params_dict = { 
                'family': 'binomial',
                'key': key,
                'y': y,
                'x': x,
                # 'case" is apparently used for matching against an output value
                # it needs to be something..anything none matching is forced to 0? 
                # used to create binary choices in output for logistic regression
                # FIX! default is bad if you have no 1's in your data. (like 2 and -2
                'case': 'NaN',
                # just want to try this out to see if legal
                'link': 'familyDefault'
            }
        else:
            params_dict = { 
                'family': 'binomial',
                'Key': key,
                'Y': 1,
            }

        # special case these two because of name issues.
        # use glm_lamba, not lambda. use glm_notX, not -X
        glm_lambda = kwargs.pop('glm_lambda', None)
        if glm_lambda is not None: params_dict['lambda'] = glm_lambda

        # hackery for new/old transition, accept either
        glm_notX = kwargs.pop('glm_-X', None)
        if not glm_notX:
            glm_notX = kwargs.pop('glm_-x', None)

        if new_json:
            if glm_notX is not None: params_dict['-x'] = glm_notX
        else:
            if glm_notX is not None: params_dict['-X'] = glm_notX

        params_dict.update(kwargs)
        print "GLM params list", params_dict

        a = self.__check_request(requests.get(
            self.__url(parentName + '.json', new=new_json),
            timeout=timeoutSecs,
            params=params_dict))
        
        verboseprint(parentName, dump_json(a))
        return a 

    def GLM(self, key, timeoutSecs=300, retryDelaySecs=0.5, **kwargs):
        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, parentName="GLM", **kwargs)

        # placeholder
        # FIX! seems like GLMProgress doesn't exist yet? at least not a redirect
        # note there is no RF redirect to RFView?
        if new_json and 1==0:
            # Check that the response has the right GLMProgress url it's going to steer us to.
            if a['response']['redirect_request']!='GLMProgress':
                print dump_json(a)
                raise Exception('H2O GLM redirect is not GLMProgress. GLMGrid json response precedes.')
            a = self.poll_url(a['response'], timeoutSecs, retryDelaySecs)
            verboseprint("GLM done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            # FIX! GLMProgress doesn't exist yet.
            if new_json:
                print "Redoing (in Parallel?) the GLM through the browser, no results saved though"
                print "How come no GLMProgress for long GLMs?"
                # find a match on the first. Swap in the 2nd to the url (as well as xlate to html)
                # because we don't want to restart the GLM?
                h2b.browseJsonHistoryAsUrlLastMatch('GLM')
            else:
                print "Redoing the GLM through the browser, no results saved though"
                h2b.browseJsonHistoryAsUrlLastMatch('GLM')
            # wait so we can see it
            time.sleep(5)
        return a

    # this only exists in new. old will fail
    def GLMGrid(self, key, timeoutSecs=300, retryDelaySecs=1.0, **kwargs):
        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, parentName="GLMGrid", **kwargs)

        # Check that the response has the right ParseProgress url it's going to steer us to.
        if a['response']['redirect_request']!='GLMGridProgress':
            print dump_json(a)
            raise Exception('H2O GLMGrid redirect is not GLMGridProgress. GLMGrid json response precedes.')
        a = self.poll_url(a['response'], timeoutSecs, retryDelaySecs)
        verboseprint("GLMGrid done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Viewing the GLM grid result through the browser"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMGridProgress')
            time.sleep(5)
        return a

    def stabilize(self, test_func, error,
            timeoutSecs=10, retryDelaySecs=0.5):
        '''Repeatedly test a function waiting for it to return True.

        Arguments:
        test_func      -- A function that will be run repeatedly
        error          -- A function that will be run to produce an error message
                          it will be called with (node, timeTakenSecs, numberOfRetries)
                    OR
                       -- A string that will be interpolated with a dictionary of
                          { 'timeTakenSecs', 'numberOfRetries' }
        timeoutSecs    -- How long in seconds to keep trying before declaring a failure
        retryDelaySecs -- How long to wait between retry attempts
        '''
        start = time.time()
        numberOfRetries = 0
        while time.time() - start < timeoutSecs:
            if test_func(self):
                break
            time.sleep(retryDelaySecs)
            numberOfRetries += 1
        else:
            timeTakenSecs = time.time() - start
            if isinstance(error, type('')):
                raise Exception('%s failed after %.2f seconds having retried %d times' % (
                            error, timeTakenSecs, numberOfRetries))
            else:
                msg = error(self, timeTakenSecs, numberOfRetries)
            raise Exception(msg)

    def wait_for_node_to_accept_connections(self,timeoutSecs=15):
        verboseprint("wait_for_node_to_accept_connections")
        def test(n):
            try:
                n.get_cloud()
                return True
            except requests.ConnectionError, e:
                # Connection refusal is normal. 
                # It just means the node has not started up yet.
                conn_err = e.args[0].errno
                if (    conn_err == 61 or   # mac/linux
                        conn_err == 54 or
                        conn_err == 111 or  # mac/linux
                        conn_err == 104 or  # ubuntu (kbn)
                        conn_err == 10061): # windows
                    return False
                verboseprint("Connection error", conn_err, 
                    "during wait_for_node_to_accept_connections")
                # 110 is a timeout: I'm getting sometimes from my ubuntu to centos
                # if there's a raise, we end up waiting for timeout before seeing it!
                raise

        self.stabilize(test, 'Cloud accepting connections',
                timeoutSecs=timeoutSecs, # with cold cache's this can be quite slow
                retryDelaySecs=0.1) # but normally it is very fast

    def get_args(self):
        #! FIX! is this used for both local and remote? 
        # I guess it doesn't matter if we use flatfile for both now
        args = [ 'java' ]

        # defaults to not specifying
        if self.java_heap_GB is not None:
            if (1 > self.java_heap_GB > 20):
                raise Exception('java_heap_GB <1 or >12 (GB): %s' % (self.java_heap_GB))
            args += [ '-Xmx%dG' % self.java_heap_GB ]

        if self.use_debugger:
            args += ['-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000']
        args += ["-ea"]
        if self.classpath:
            entries = [ find_file('build/classes'), find_file('lib/javassist.jar') ] 
            entries += glob.glob(find_file('lib')+'/*/*.jar')
            entries += glob.glob(find_file('lib')+'/*/*/*.jar')
            args += ['-classpath', os.pathsep.join(entries), 'init.Boot']
        else: 
            args += ["-jar", self.get_h2o_jar()]
        args += [
            "--port=%d" % self.port,
            '--ip=%s' % self.addr,
            '--ice_root=%s' % self.get_ice_dir(),
            # if I have multiple jenkins projects doing different h2o clouds, I need
            # I need different ports and different cloud name.
            # does different cloud name prevent them from joining up 
            # (even if same multicast ports?)
            # I suppose I can force a base address. or run on another machine?
            '--name=' + cloud_name()
            ]

        # FIX! need to be able to specify name node/path for non-0xdata hdfs
        # specifying hdfs stuff when not used shouldn't hurt anything
        if self.use_hdfs:
            args += [
                '-hdfs hdfs://' + self.hdfs_name_node,
                '-hdfs_version ' + self.hdfs_version, 
                '-hdfs_root ' + self.hdfs_root
            ]
            if self.hdfs_nopreload:
                args += [
                    '-hdfs_nopreload ' + self.hdfs_nopreload
                ]

        if self.use_flatfile:
            args += [
                '--flatfile=' + self.flatfile,
            ]

        if not self.sigar:
            args += ['--nosigar']
        return args

    def __init__(self, 
        use_this_ip_addr=None, port=54321, capture_output=True, sigar=False, use_debugger=None, classpath=None,
        use_hdfs=False, hdfs_name_node="192.168.1.151", hdfs_root="/datasets", hdfs_version="cdh4",
        hdfs_nopreload=None,
        use_flatfile=False, java_heap_GB=None, use_home_for_ice=False, node_id=None, username=None):

        if use_debugger is None: use_debugger = debugger
        if use_this_ip_addr is None: use_this_ip_addr = get_ip_address()

        self.port = port
        self.addr = use_this_ip_addr
        self.sigar = sigar
        self.use_debugger = use_debugger
        self.classpath = classpath
        self.capture_output = capture_output

        self.use_hdfs = use_hdfs
        self.hdfs_name_node = hdfs_name_node
        self.hdfs_version = hdfs_version
        self.hdfs_root = hdfs_root
        self.hdfs_nopreload = hdfs_nopreload

        self.use_flatfile = use_flatfile
        self.java_heap_GB = java_heap_GB

        self.use_home_for_ice = use_home_for_ice
        self.node_id = node_id
        self.username = username

    def __str__(self):
        return '%s - http://%s:%d/' % (type(self), self.addr, self.port)

    def get_ice_dir(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def get_h2o_jar(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def is_alive(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def terminate(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

class ExternalH2O(H2O):
    '''An H2O instance launched outside the control of python'''
    def __init__(self, *args, **kwargs):
        super(ExternalH2O, self).__init__(*args, **kwargs)

    def get_h2o_jar(self):
        return find_file('build/h2o.jar') # just a likely guess

    def get_ice_dir(self):
        return '/tmp/ice%d' % self.port # just a likely guess

    def is_alive(self):
        try:
            self.get_cloud()
            return True
        except:
            return False

    def terminate(self):
        # try/except for this is inside shutdown_all now
        self.shutdown_all()
        if self.is_alive():
            raise 'Unable to terminate externally launched node: %s' % self


class LocalH2O(H2O):
    '''An H2O instance launched by the python framework on the local host using psutil'''
    def __init__(self, *args, **kwargs):
        super(LocalH2O, self).__init__(*args, **kwargs)
        self.rc = None
        # FIX! no option for local /home/username ..always the sandbox (LOG_DIR)
        self.ice = tmp_dir('ice.')
        self.flatfile = flatfile_name()
        if self.node_id is not None:
            logPrefix = 'local-h2o-' + str(self.node_id)
        else:
            logPrefix = 'local-h2o'
        check_port_group(self.port)
        spawn = spawn_cmd(logPrefix, self.get_args(), capture_output=self.capture_output)
        self.ps = spawn[0]

    def get_h2o_jar(self):
        return find_file('build/h2o.jar')

    def get_flatfile(self):
        return self.flatfile
        # return find_file(flatfile_name())

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        verboseprint("Doing is_alive check for LocalH2O", self.wait(0))
        return self.wait(0) is None
    
    def terminate(self):
        # send a shutdown request first. This matches ExternalH2O
        # since local is used for a lot of buggy new code, also do the ps kill.
        # try/except inside shutdown_all now
        self.shutdown_all()

        # we need a delay after shutdown_all above, before this check?
        time.sleep(1)
        if self.is_alive():
            print "\nShutdown didn't work for local node? : %s. Will kill though" % self

        try:
            if self.is_alive(): self.ps.kill()
            if self.is_alive(): self.ps.terminate()
            return self.wait(0.5)
        except psutil.NoSuchProcess:
            return -1

    def wait(self, timeout=0):
        if self.rc is not None: return self.rc
        try:
            self.rc = self.ps.wait(timeout)
            return self.rc
        except psutil.TimeoutExpired:
            return None

    def stack_dump(self):
        self.ps.send_signal(signal.SIGQUIT)

class RemoteHost(object):
    def upload_file(self, f, progress=None):
        # FIX! we won't find it here if it's hdfs://192.168.1.151/ file
        f = find_file(f)
        if f not in self.uploaded:
            import md5
            m = md5.new()
            m.update(open(f).read())
            m.update(getpass.getuser())
            dest = '/tmp/' +m.hexdigest() +"-"+ os.path.basename(f)

            # sigh. we rm/create sandbox in build_cloud now 
            # (because nosetests doesn't exec h2o_main and we 
            # don't want to code "clean_sandbox()" in all the tests.
            # So: we don't have a sandbox here, or if we do, we're going to delete it.
            # Just don't log anything until build_cloud()? that should be okay?
            # we were just logging this upload message..not needed.
            # log('Uploading to %s: %s -> %s' % (self.addr, f, dest))

            sftp = self.ssh.open_sftp()
            sftp.put(f, dest, callback=progress)
            sftp.close()
            self.uploaded[f] = dest
        return self.uploaded[f]

    def record_file(self, f, dest):
        '''Record a file as having been uploaded by external means'''
        self.uploaded[f] = dest

    def run_cmd(self, cmd):
        log('Running `%s` on %s' % (cmd, self))
        (stdin, stdout, stderr) = self.ssh.exec_command(cmd)
        stdin.close()

        sys.stdout.write(stdout.read())
        sys.stdout.flush()
        stdout.close()

        sys.stderr.write(stderr.read())
        sys.stderr.flush()
        stderr.close()

    def push_file_to_remotes(self, f, hosts):
        dest = self.uploaded[f]
        for h in hosts:
            if h == self: continue
            self.run_cmd('scp %s %s@%s:%s' % (dest, h.username, h.addr, dest))
            h.record_file(f, dest)

    def __init__(self, addr, username, password=None, **kwargs):
        import paramiko
        self.addr = addr
        self.username = username
        self.ssh = paramiko.SSHClient()

        # don't require keys. If no password, assume passwordless setup was done
        policy = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(policy)
        self.ssh.load_system_host_keys()
        if password is None:
            self.ssh.connect(self.addr, username=username, **kwargs)
        else:
            self.ssh.connect(self.addr, username=username, password=password, **kwargs)

        self.uploaded = {}

    def remote_h2o(self, *args, **kwargs):
        return RemoteH2O(self, self.addr, *args, **kwargs)

    def open_channel(self):
        # kbn
        # ch = self.ssh.invoke_shell()
        ch = self.ssh.get_transport().open_session()
        ch.get_pty() # force the process to die without the connection
        return ch

    def __str__(self):
        return 'ssh://%s@%s' % (self.username, self.addr)


class RemoteH2O(H2O):
    '''An H2O instance launched by the python framework on a specified host using openssh'''
    def __init__(self, host, *args, **kwargs):
        super(RemoteH2O, self).__init__(*args, **kwargs)

        self.jar = host.upload_file('build/h2o.jar')
        # need to copy the flatfile. We don't always use it (depends on h2o args)
        self.flatfile = host.upload_file(flatfile_name())

        if self.use_home_for_ice:
            # this will be the username used to ssh to the host
            self.ice = "/home/" + host.username + '/ice.%d.%s' % (self.port, time.time())
        else:
            self.ice = '/tmp/ice.%d.%s' % (self.port, time.time())

        self.channel = host.open_channel()
        ### FIX! TODO...we don't check on remote hosts yet
       
        # this fires up h2o over there
        cmd = ' '.join(self.get_args())
        self.channel.exec_command(cmd)
        if self.capture_output:
            if self.node_id is not None:
                logPrefix = 'remote-h2o-' + str(self.node_id)
            else:
                logPrefix = 'remote-h2o'

            outfd,outpath = tmp_file(logPrefix + '.stdout.', '.log')
            errfd,errpath = tmp_file(logPrefix + '.stderr.', '.log')

            drain(self.channel.makefile(), outfd)
            drain(self.channel.makefile_stderr(), errfd)
            comment = 'Remote on %s, stdout %s, stderr %s' % (
                self.addr, os.path.basename(outpath), os.path.basename(errpath))
        else:
            drain(self.channel.makefile(), sys.stdout)
            drain(self.channel.makefile_stderr(), sys.stderr)
            comment = 'Remote on %s' % self.addr

        log(cmd, comment=comment)

    def get_h2o_jar(self):
        return self.jar

    def get_flatfile(self):
        return self.flatfile

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        verboseprint("Doing is_alive check for RemoteH2O")
        if self.channel.closed: return False
        if self.channel.exit_status_ready(): return False
        try:
            self.get_cloud()
            return True
        except:
            return False

    def terminate(self):
        self.shutdown_all()
        self.channel.close()
        # kbn: it should be dead now? want to make sure we don't have zombies
        # we should get a connection error. doing a is_alive subset.
        try:
            gc_output = self.get_cloud()
            raise "get_cloud() should fail after we terminate a node. It isn't. %s %s" % (self, gc_output)
        except:
            return True
    
