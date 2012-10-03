import time, os, json, signal, tempfile, shutil, datetime, inspect, threading, os.path, getpass
import requests, psutil

def __drain(src, dst):
    for l in src:
        if type(dst) == type(0):
            os.write(dst, l)
        else:
            dst.write(l)
            dst.flush()

def drain(src, dst):
    t = threading.Thread(target=__drain, args=(src,dst))
    t.daemon = True
    t.start()

def verboseprint(*args):
    ### global verbose
    if 1==0: # change to 1==1 if you want verbose
        for arg in args: # so you don't have to create a single string
            print arg,
        print

def find_file(base):
    f = base
    if not os.path.exists(f): f = '../'+base
    return f

LOG_DIR = 'sandbox'
def clean_sandbox():
    if os.path.exists(LOG_DIR):
        # shutil.rmtree fails to delete very long filenames on Windoze
        #shutil.rmtree(LOG_DIR)
        # This seems reliable on windows+cygwin
        os.system("rm -rf "+LOG_DIR)
    os.mkdir(LOG_DIR)

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

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
# Will fail if local proxy? we don't have one.
# Watch out to see if there are NAT issues here (home router?)
# Could parse ifconfig, but would need something else on windows
def get_ip_address():
    import socket
    ip = '127.0.0.1'
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',0))
        ip = s.getsockname()[0]
        verboseprint("ip1:", ip)
    except:
        pass

    if ip.startswith('127'):
        ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]

    verboseprint("get_ip_address:", ip) 
    return ip

def spawn_cmd(name, args):
    outfd,outpath = tmp_file(name + '.stdout.', '.log')
    errfd,errpath = tmp_file(name + '.stderr.', '.log')
    ps = psutil.Popen(args, stdin=None, stdout=outfd, stderr=errfd)
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

nodes = []
def build_cloud(node_count, base_port=54321, ports_per_node=3, addr=None, sigar=True):
    node_list = []
    try:
        for i in xrange(node_count):
            n = LocalH2O(addr, port=base_port + i*ports_per_node, sigar=sigar)
            node_list.append(n)

        stabilize_cloud(node_list[0], len(node_list))
    except:
        for n in node_list: n.terminate()
        raise
    nodes[:] = node_list
    return node_list

def tear_down_cloud(node_list=None):
    if not node_list: node_list = nodes
    try:
        for n in node_list:
            n.terminate()
    finally:
        node_list[:] = []

def stabilize_cloud(node, node_count, timeoutSecs=10.0, retryDelaySecs=0.25):
    node.wait_for_node_to_accept_connections()
    node.stabilize(lambda n: n.get_cloud()['cloud_size'] == node_count,
            error=('A cloud of size %d' % node_count),
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

class H2O(object):
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __check_request(self, r):
        log('Sent ' + r.url)
        if not r:
            raise Exception('Error in %s: %s' % (inspect.stack()[1][3], str(r)))
        # json name used in import
        rjson = r.json
        if 'error' in rjson:
            raise Exception('Error in %s: %s' % (inspect.stack()[1][3], rjson['error']))
        return rjson

    def get_cloud(self):
        a = self.__check_request(requests.get(self.__url('Cloud.json')))
        verboseprint ("get_cloud:", a)
        return a

    def shutdown_all(self):
        return self.__check_request(requests.get(self.__url('Shutdown.json')))

    def put_value(self, value, key=None, repl=None):
        return self.__check_request(
            requests.get(self.__url('PutValue.json'), 
                params={"Value": value, "Key": key, "RF": repl}
                ))

    def put_file(self, f, key=None, repl=None):
        return self.__check_request(
            requests.post(self.__url('PutFile.json'), 
                files={"File": open(f, 'rb')},
                params={"Key": key, "RF": repl} # key is optional. so is repl factor (called RF)
                ))

    # FIX! placeholder..what does the JSON really want?
    def get_file(self, f):
        return self.__check_request(requests.post(self.__url('GetFile.json'), 
            files={"File": open(f, 'rb')}))

    def parse(self, key):
        return self.__check_request(requests.get(self.__url('Parse.json'),
            params={"Key": key}))

    def netstat(self):
        return self.__check_request(requests.get(self.__url('Network.json')))

    def inspect(self, key):
        return self.__check_request(requests.get(self.__url('Inspect.json'),
            params={"Key": key}))

    def random_forest(self, key, ntrees=6, depth=30):
        return self.__check_request(requests.get(self.__url('RF.json'),
            params={
                "depth": depth,
                "ntrees": ntrees,
                "Key": key
                }))

    def random_forest_view(self, key):
        a = self.__check_request(requests.get(self.__url('RFView.json'),
            params={"Key": key}))
        verboseprint("random_forest_view:", a)
        return a

    def linear_reg(self, key, colA=0, colB=1):
        a = self.__check_request(requests.get(self.__url('LR.json'),
            params={
                "colA": colA,
                "colB": colB,
                "Key": key
                }))
        verboseprint("linear_reg:", a)
        return a

    def linear_reg_view(self, key):
        a = self.__check_request(requests.get(self.__url('LRView.json'),
            params={"Key": key}))
        verboseprint("linear_reg_view:", a)
        return a

    # X and Y can be label strings, column nums, or comma separated combinations
    def GLM(self, key, X="0", Y="1", family="binomial"):
        a = self.__check_request(requests.get(self.__url('GLM.json'),
            params={
                "family": family,
                "X": X,
                "Y": Y,
                "Key": key
                }))
        verboseprint("GLM:", a)
        return a

    def GLM_view(self, key):
        a = self.__check_request(requests.get(self.__url('GLMView.json'),
            params={"Key": key}))
        verboseprint("GLM_view:", a)
        return a

    def stabilize(self, test_func, error,
            timeoutSecs, retryDelaySecs=0.5):
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

    def wait_for_node_to_accept_connections(self):
        def test(n):
            try:
                n.get_cloud()
                return True
            except requests.ConnectionError, e:
                # Connection refusal is normal. 
                # It just means the node has not started up yet.
                if (    e.args[0].errno == 61 or   # mac/linux
                        e.args[0].errno == 111 or  # mac/linux
                        e.args[0].errno == 10061): # windows
                    return False
                raise
        self.stabilize(test, 'Cloud accepting connections',
                timeoutSecs=10, # with cold cache's this can be quite slow
                retryDelaySecs=0.1) # but normally it is very fast

    def get_args(self):
        args = [ "java", 
            "-javaagent:" + self.get_h2o_jar(),
            "-ea", "-jar", self.get_h2o_jar(),
            "--port=%d" % self.port,
            '--ip=%s' % self.addr,
            '--ice_root=%s' % self.get_ice_dir(),
            '--name=pytest-%s' % getpass.getuser(),
            ]
        if not self.sigar:
            args.append('--nosigar')
        return args

    def __init__(self, addr=None, port=54321, sigar=False):
        self.port = port
        self.addr = addr or get_ip_address()
        self.sigar = sigar

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
    def __init__(self, addr=None, port=54321, sigar=True):
        super(ExternalH2O, self).__init__(addr, port, sigar=sigar)

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
        try:
            self.shutdown_all()
        except:
            pass
        if self.is_alive():
            raise 'Unable to terminate externally launched node: %s' % self


class LocalH2O(H2O):
    '''An H2O inkstance launched by the python framework on the local machine'''
    def __init__(self, addr=None, port=54321, sigar=True):
        super(LocalH2O, self).__init__(addr, port, sigar=sigar)
        self.rc = None
        self.ice = tmp_dir('ice.')

        spawn = spawn_cmd('local-h2o', self.get_args())
        self.ps = spawn[0]
        self.stdout = spawn[1]
        self.stderr = spawn[1]


    def get_h2o_jar(self):
        return find_file('build/h2o.jar')

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        return self.wait(0) is None
    
    def terminate(self):
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

class RemoteH2O(H2O):
    def __upload_file(self, f):
        f = find_file(f)
        dest = '/tmp/' + os.path.basename(f)
        log('Uploading to %s: %s -> %s' % (self.addr, f, dest))
        sftp = self.ssh.open_sftp()
        sftp.put(f, dest)
        sftp.close()
        return dest

    '''An H2O instance launched by the python framework on a remote machine'''
    def __init__(self, addr=None, port=54321, sigar=True, username=None):
        super(RemoteH2O, self).__init__(addr, port, sigar=sigar)
        import paramiko
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.addr, username=username)

        self.jar = self.__upload_file(find_file('build/h2o.jar'))
        self.ice = '/tmp/ice.%d.%s' % (self.port, time.time())

        self.channel = self.ssh.get_transport().open_session()
        self.channel.get_pty() # force the process to die without the connection

        cmd = ' '.join(self.get_args())
        outfd,outpath = tmp_file('remote-h2o.stdout.', '.log')
        errfd,errpath = tmp_file('remote-h2o.stderr.', '.log')
        self.channel.exec_command(cmd)
        drain(self.channel.makefile(), outfd)
        drain(self.channel.makefile_stderr(), errfd)

        comment = 'Remote on %s, stdout %s, stderr %s' % (
            self.addr, os.path.basename(outpath), os.path.basename(errpath))
        log(cmd, comment=comment)

    def get_h2o_jar(self):
        return self.jar

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        if self.channel.closed: return False
        if self.channel.exit_status_ready(): return False
        try:
            self.get_cloud()
            return True
        except:
            return False

    def terminate(self):
        self.channel.close()
    