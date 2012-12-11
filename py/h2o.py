import time, os, json, signal, tempfile, shutil, datetime, inspect, threading, os.path, getpass
import requests, psutil, argparse, sys, unittest
import glob
import h2o_browse as h2b
import re
import inspect

# pytestflatfile name
# the cloud is uniquely named per user (only)
# if that's sufficient, it should be fine to uniquely identify the flatfile by name only also
# (both are the user that runs the test. Note the config might have a different username on the
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
    print "\nRunning python:", inspect.stack()[1][1]
    clean_sandbox()
    parse_our_args()
    unittest.main()


browse_json = False
verbose = False
ipaddr = None
use_hosts = False
debugger=False

def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('--browse_json','-b', help='Pops a browser to selected json equivalent urls. Selective. Also keeps test alive (and H2O alive) till you ctrl-c. Then should do clean exit', action='store_true')
    parser.add_argument('--verbose','-v', help='increased output', action='store_true')
    parser.add_argument('--ip', type=str, help='IP address to use for single host H2O with psutil control')
    parser.add_argument('--use_hosts', '-uh', help='pending...intent was conditional hosts use', action='store_true')
    parser.add_argument('--debugger', help='Launch java processes with java debug attach mechanisms', action='store_true')
    
    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    global browse_json, verbose, ipaddr, use_hosts, debugger
    browse_json = args.browse_json
    verbose = args.verbose
    ipaddr = args.ip
    use_hosts = args.use_hosts
    debugger = args.debugger

    # set sys.argv to the unittest args (leav sys.argv[0] as is)
    # FIX! this isn't working to grab the args we don't care about
    # pass "-f" to stop on first error to unittest. and -v
    # We want this to be standard, always (note -f for unittest, nose uses -x?)
    # sys.argv[1:] = ["-v", "--failfast"] + args.unittest_args
    # kbn: disabling failfast until we fix jenkins
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
    while not os.path.exists(os.path.join(head, tail)):
        head = os.path.split(head)[0]
    return os.path.join(head, tail, f)

def find_file(base):
    f = base
    if not os.path.exists(f): f = '../'+base
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

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
# Will fail if local proxy? we don't have one.
# Watch out to see if there are NAT issues here (home router?)
# Could parse ifconfig, but would need something else on windows
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

# this is used by tests, to create hdfs URIs. it will always get set to the name node we're using
# if any. (in build_cloud)
global use_hdfs
use_hdfs = False

global hdfs_name_node
hdfs_name_node = "192.168.1.151"

def write_flatfile(node_count=2, base_port=54321, hosts=None):
    # we're going to always create the flatfile. 
    # Used for all remote cases now. (per sri)
    pff = open(flatfile_name(), "w+")
    if hosts is None:
        ip = get_ip_address()
        for i in xrange(node_count):
            pff.write("/" + ip + ":" + str(base_port +3*i) + "\n")
    else:
        for h in hosts:
            for i in xrange(node_count):
                pff.write("/" + h.addr + ":" + str(base_port +3*i) + "\n")
    pff.close()

# node_count is per host if hosts is specified.
# FIX! should rename node_count to nodes_per_host, but have to fix all tests that keyword it.
def build_cloud(node_count=2, base_port=54321, hosts=None, 
        timeoutSecs=20, retryDelaySecs=0.5, cleanup=True, **kwargs):
    global nodes, use_hdfs, hdfs_name_node

    # set the hdfs info that tests will use from kwargs
    # the philosopy is that kwargs holds stuff that's used for node level building.
    if "use_hdfs" in kwargs:
        use_hdfs = kwargs["use_hdfs"]
        verboseprint("use_hdfs passed to build_cloud:", use_hdfs)

    if "hdfs_name_node" in kwargs:
        hdfs_name_node = kwargs["hdfs_name_node"]
        verboseprint("hdfs_name_node passed to build_cloud:", hdfs_name_node)

    # hardwire this. don't need it to be an arg
    ports_per_node = 3
    node_list = []
    try:
        # if no hosts list, use psutil method on local host.
        totalNodes = 0
        if hosts is None:
            hostCount = 1
            for i in xrange(node_count):
                verboseprint('psutil starting node', i)
                newNode = LocalH2O(port=base_port + i*ports_per_node, **kwargs)
                node_list.append(newNode)
                totalNodes += 1
        else:
            hostCount = len(hosts)
            for h in hosts:
                for i in xrange(node_count):
                    verboseprint('ssh starting node', i, 'via', h)
                    newNode = h.remote_h2o(port=base_port + i*ports_per_node, **kwargs)
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
        # For now, only do this for remote case. It's a good check too, for the more stressful
        # remote cases
        if hosts is not None:
            for n in nodes:
                stabilize_cloud(n, len(nodes), timeoutSecs=15)

    except:
        if cleanup:
            for n in node_list: n.terminate()
        else:
            nodes[:] = node_list
        check_sandbox_for_errors()
        raise

    # this is just in case they don't assign the return to the nodes global?
    nodes[:] = node_list
    return node_list

def upload_jar_to_remote_hosts(hosts, slow_connection=False):
    def prog(sofar, total):
        p = int(10.0 * sofar / total)
        sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#'*p, ' '*(10-p), 100*sofar/total))
        sys.stdout.flush()
        
    if not slow_connection:
        for h in hosts:
            h.upload_file('build/h2o.jar', progress=prog)
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
    # If timeouts are tuned reasonably, we'll get here quick
    # There's a way to run nosetest to stop on first subtest error..Maybe we should do that.

    # if you find a problem, just keep printing till the end
    # in that file. 
    # The stdout/stderr is shared for the entire cloud session?
    # so don't want to dump it multiple times?
    for filename in os.listdir(LOG_DIR):
        if re.search('stdout|stderr',filename):
            sandFile = open(LOG_DIR + "/" + filename, "r")
            # just in case rror/ssert is lower or upper case
            # FIX! aren't we going to get the cloud building info failure messages
            # oh well...if so ..it's a bug! "killing" is temp to detect jar mismatch error
            regex = re.compile('exception|error|assert|warn|info|killing|killed|required ports',re.IGNORECASE)
            printing = 0
            for line in sandFile:
                newFound = regex.search(line) and ('error rate' not in line)
                if (printing==0 and newFound):
                    printing = 1
                elif (printing==1):
                    # if we've been printing, stop when you get to another error
                    # we don't care about seeing multiple prints scroll off the screen
                    if (newFound):
                        printing = 2 

                if (printing==1):
                    # to avoid extra newline from print. line already has one
                    sys.stdout.write(line)
        
            sandFile.close()

    return (printing!=0) # can test and cause exception


def tear_down_cloud(node_list=None):
    if not node_list: node_list = nodes
    try:
        for n in node_list:
            n.terminate()
            verboseprint("tear_down_cloud n:", n)
    finally:
        node_list[:] = []
        check_sandbox_for_errors()

# REQUIRED IN EACH TEST: have to touch something otherwise the ssh channel shuts down
# and terminates the H2O. We're using RemoteH2O which keeps H2O there only 
# while ssh channel is live. (good for avoiding orphans out of our test control)
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
        cloud_size = c['cloud_size']
        consensus = c['consensus']

        if (cloud_size > node_count):
            emsg = (
                "\n\nERROR: cloud_size: %d reported via json is bigger than we expect: %d" % (cloud_size, node_count) +
                "\nYou likely have zombie(s) with the same cloud name on the network, that's forming up with you." +
                "\nLook at the cloud IP's in 'grep Paxos sandbox/*stdout*' for some IP's you didn't expect." +
                "\n\nYou probably don't have to do anything, as the cloud shutdown in this test should"  +
                "\nhave sent a Shutdown.json to all in that cloud (you'll see a kill -2 in the *stdout*)." +
                "\nIf you try again, and it still fails, go to those IPs and kill the zombie h2o's." +
                "\nIf you think you really have an intermittent cloud build, report it."
                )
            raise Exception(emsg)

        a = (cloud_size==node_count and consensus)
        ### verboseprint("at stabilize_cloud:", cloud_size, node_count, consensus, a)
        return(a)

    node.stabilize(test, error=('A cloud of size %d' % node_count),
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

class H2O(object):
    def __url(self, loc, port=None):
        if port is None: port = self.port
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

        # HACK: excessive variance in GLM/RF/other..so check a bunch...should jira this issue
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


    def get_cloud(self):
        a = self.__check_request(requests.get(self.__url('Cloud.json')))
        verboseprint("get_cloud:", a)
        return a

    def get_timeline(self):
        return self.__check_request(requests.get(self.__url('Timeline.json')))

    # Shutdown url is like a reset button. Doesn't send a response before it kills stuff
    # safer if random things are wedged, rather than requiring response
    # so request library might retry and get exception. allow that.
    def shutdown_all(self):
        try:
            self.__check_request(requests.get(self.__url('Shutdown.json')))
        except:
            pass
        return(True)

    def put_value(self, value, key=None, repl=None):
        return self.__check_request(
            requests.get(
                self.__url('PutValue.json'), 
                params={"Value": value, "Key": key, "RF": repl}),
            extraComment = str(value) + "," + str(key) + "," + str(repl))

    def put_file_old(self, f, key=None, repl=None):
        return self.__check_request(
            requests.post(
                self.__url('PutFile.json'), 
                files={"File": open(f, 'rb')},
                params={"Key": key, "RF": repl}), # key is optional. so is repl factor (called RF)
            extraComment = str(f) + "," + str(key) + "," + str(repl))

    def put_file(self, f, key=None, repl=None):
        resp1 =  self.__check_request(
            requests.get(
                self.__url('PutFile.json'), 
                params={"Key": key, "RF": repl}), # key is optional. so is repl factor (called RF)
            extraComment = str(f) + "," + str(key) + "," + str(repl))

        verboseprint("\nput_file #1 phase response: ", resp1)
        resp2 = self.__check_request(
            requests.post(
                self.__url('Upload.json', port=resp1['port']), 
                files={"File": open(f, 'rb')}),
            extraComment = str(f))

        verboseprint("put_file #2 phase response: ", resp2)

        return resp2[0]
    
    def get_key(self, key):
        return requests.get(self.__url('Get'),
            prefetch=False,
            params={"Key": key})

    # FIX! placeholder..what does the JSON really want?
    def get_file(self, f):
        a = self.__check_request(
            requests.post(
                self.__url('GetFile.json'), 
                files={"File": open(f, 'rb')}),
            extraComment = str(f))
        verboseprint("\nget_file result:", dump_json(a))
        return a

    # FIX! TEMP: right now H2O does blocking response ..i.e. we get nothing until 
    # the parse is done. Parse can take a long time. Should be intermediate views of something?
    # if we timeout repeatedly, we can exceed the default retry count in the requests library
    # set retries and timeout specifically here, so we can learn more about this
    # timeout has to be big to cover longest expected parse? timeout is float. secs?
    # looks like max_retries is part of configuration defaults
    # maybe we should limit retries everywhere, for better visibiltiy into intermmitent H2O rejects?
    def parse(self, key, key2=None, timeoutSecs=300):
        # this doesn't work. webforums indicate max_retries might be 0 already? (as of 3 months ago)
        # requests.defaults({max_retries : 4})
        # https://github.com/kennethreitz/requests/issues/719
        # it was closed saying Requests doesn't do retries. (documentation implies otherwise)
        # don't need extraComment because
        a = self.__check_request(
            requests.get(
                url=self.__url('Parse.json'),
                timeout=timeoutSecs,
                params={"Key": key, "Key2": key2}))
        verboseprint("\nparse result:",dump_json(a))
        return a

    def netstat(self):
        return self.__check_request(requests.get(self.__url('Network.json')))

    def jstack(self):
        return self.__check_request(requests.get(self.__url("JStack.json")))

    def inspect(self, key):
        a = self.__check_request(requests.get(self.__url('Inspect.json'),
            params={"Key": key}))
        ### verboseprint("\ninspect result:", dump_json(a))
        return a

    def import_folder(self, folder, repl=None):
        a = self.__check_request(requests.get(
            self.__url('ImportFolder.json'),
            params={
                "Folder": folder,
                "rf": repl}))
        verboseprint("\nimport_folder result:", dump_json(a))
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
        params_dict = {
            'Key' : key,
            'ntree' : trees,
            'modelKey' : 'pytest_model',
            'depth' : 30,
            'browseAlso' : False,
            }
        
        clazz = kwargs.pop('clazz', None)
        if clazz is not None: params_dict['class'] = clazz
        
        params_dict.update(kwargs)

        verboseprint("\nrandom_forest parameters:", params_dict)
        a = self.__check_request(requests.get(
            url=self.__url('RF.json'), 
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
    # UPDATE: jan says the ignore should be picked up from the model
    def random_forest_view(self, dataKey, modelKey, ntree, timeoutSecs=300, **kwargs):

        # FIX! maybe we should pop off values from kwargs that RFView is not supposed to need?
        # that would make sure we only pass the minimal?
        # Note ntree in kwargs can overwrite trees! We use this for random param generation

        # UPDATE: only pass the minimal set of params to RFView. It should get the 
        # rest from the model. what about classWt? It can be different between RF and RFView?
        # Will need to update this list if we params for RfView
        params_dict = {
            'dataKey' : dataKey,
            'modelKey' : modelKey,
            'OOBEE' : None,
            'classWt' : None
            }
        # only update params_dict..don't add
        # throw away anything else as it should come from the model (propagating what RF used)
        for k in kwargs:
            if k in params_dict:
                params_dict[k] = kwargs[k]

        browseAlso = kwargs.pop('browseAlso',False)

        a = self.__check_request(requests.get(
            self.__url('RFView.json'),
            timeout=timeoutSecs,
            params=params_dict))

        verboseprint("\nrandom_forest_view result:", dump_json(a))
        # we should know the json url from above, but heck lets just use
        # the same history-based, global mechanism we use elsewhere
        # look at the passed down enable, or the global args
        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
        return a

    def linear_reg(self, key, timeoutSecs=10, **kwargs):
        params_dict = {
            'Key' : key,
            'colA' : 0,
            'colB' : 1,
            }
        params_dict.update(kwargs)
        a = self.__check_request(
            requests.get(self.__url('LR.json'),
            timeout=timeoutSecs,
            params=params_dict))

        verboseprint("\nlinear_reg result:", dump_json(a))
        return a

    def linear_reg_view(self, key, timeoutSecs=10):
        a = self.__check_request(
            requests.get(self.__url('LRView.json'),
            timeout=timeoutSecs,
            params={'Key': key}))

        verboseprint("linear_reg_view result:", dump_json(a))
        return a

    # kwargs used to pass:
    # Y
    # X
    # -X
    # family
    # threshold
    # norm
    # glm_lambda (becomes lambda)
    # rho
    # alpha

    def GLM(self, key, timeoutSecs=300, **kwargs):
        # for defaults
        params_dict = { 
            'family': 'binomial',
            'Key': key,
            'Y': 1
            }

        # special case these two because of name issues.
        # use glm_lamba, not lambda
        # use glm_notX, not -X
        glm_lambda = kwargs.pop('glm_lambda', None)
        if glm_lambda is not None: params_dict['lambda'] = glm_lambda
        glm_notX = kwargs.pop('glm_-X', None)
        if glm_notX is not None: params_dict['-X'] = glm_notX

        params_dict.update(kwargs)
        verboseprint("GLM params list", params_dict)

        a = self.__check_request(requests.get(
            self.__url('GLM.json'), 
            timeout=timeoutSecs,
            params=params_dict))
        # remove the 'models' key that has all the CMs from cross validation..too much to print
        b = dict.copy(a)
        # if you don't do xval, there is no models, so have to check first
        if 'models' in b:
            del b['models']
        
        verboseprint("\nNot printing the CMs returned by cross validation, if any")
        verboseprint("GLM:", dump_json(b))
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
        # FIX! need to be able to specify name node/path for non-0xdata hdfs
        # specifying hdfs stuff when not used shouldn't hurt anything

        args += [
            "-ea", "-jar", self.get_h2o_jar(),
            "--port=%d" % self.port,
            '--ip=%s' % self.addr,
            '--ice_root=%s' % self.get_ice_dir(),
            # if I have multiple jenkins projects doing different h2o clouds, I need
            # I need different ports and different cloud name.
            # does different cloud name prevent them from joining up (even if same multicast ports?)
            # I suppose I can force a base address. or run on another machine?
            '--name=' + cloud_name()
            ]

        if self.use_hdfs:
            # '-hdfs_root /datasets'
            # '-hdfs_nopreload',
            args += [
                '-hdfs hdfs://' + self.hdfs_name_node,
                '-hdfs_version cdh4',
                '-hdfs_root /datasets'
            ]

            # we need a global for hdfs_name_node for tests to build up hdfs URIs.
            # They're all getting the same value
            # passed down from build_cloud_with_hosts. so use that one? or if a LocalH2O 
            # test uses just build_cloud directly. So do it up in build_cloud to handle both.

        if self.use_flatfile:
            args += [
                '--flatfile=' + self.flatfile,
            ]

        if not self.sigar:
            args += ['--nosigar']
        return args

    def __init__(self, use_this_ip_addr=None, port=54321, capture_output=True, sigar=False, 
        use_debugger=None, use_hdfs=False, hdfs_name_node="192.168.1.151", use_flatfile=False, 
        java_heap_GB=None, use_home_for_ice=False, username=None):

        if use_debugger is None: use_debugger = debugger
        if use_this_ip_addr is None: use_this_ip_addr = get_ip_address()

        self.port = port
        self.addr = use_this_ip_addr
        self.sigar = sigar
        self.use_debugger = use_debugger
        self.capture_output = capture_output
        self.use_hdfs = use_hdfs
        self.hdfs_name_node = hdfs_name_node
        self.use_flatfile = use_flatfile

        self.java_heap_GB = java_heap_GB

        self.use_home_for_ice = use_home_for_ice
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
        # FIX! no option for local /home/username ..always /tmp
        self.ice = tmp_dir('ice.')
        self.flatfile = flatfile_name()
        spawn = spawn_cmd('local-h2o', self.get_args(),
                capture_output=self.capture_output)
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
            log('Uploading to %s: %s -> %s' % (self.addr, f, dest))
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
        cmd = ' '.join(self.get_args())
        self.channel.exec_command(cmd)
        if self.capture_output:
            outfd,outpath = tmp_file('remote-h2o.stdout.', '.log')
            errfd,errpath = tmp_file('remote-h2o.stderr.', '.log')
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
        self.channel.close()
        # kbn: it should be dead now? want to make sure we don't have zombies
        # we should get a connection error. doing a is_alive subset.
        try:
            gc_output = self.get_cloud()
            raise "get_cloud() should fail after we terminate a node. It isn't. %s %s" % (self, gc_output)
        except:
            return True
    
