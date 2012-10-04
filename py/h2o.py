import time, os, json, signal, tempfile, shutil, datetime, inspect, threading, os.path, getpass
import requests, psutil, argparse, sys, unittest

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

def unit_main():
    clean_sandbox()
    parse_our_args()
    unittest.main()

verbose = False
ipaddr = None

def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('--verbose','-v', help="increased output", action="store_true")
    parser.add_argument('--ip', type=str, help="IP address to use")
    
    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    global verbose
    global ipaddr
    verbose = args.verbose
    ipaddr = args.ip

    # set sys.argv to the unittest args (leav sys.argv[0] as is)
    sys.argv[1:] = args.unittest_args

def verboseprint(*args):
    if verbose:
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
    if ipaddr:
        return ipaddr

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

def spawn_h2o(addr=None, port=54321, nosigar=True):
    h2o_cmd = [
            "java", 
            "-ea", "-jar", find_file('build/h2o.jar'),
            "--port=%d"%port,
            '--ip=%s'%(addr or get_ip_address()),
            '--ice_root=%s' % tmp_dir('ice.')
            ]
    if nosigar is True: 
        h2o_cmd.append('--nosigar')
    return spawn_cmd('h2o', h2o_cmd)

nodes = []
def build_cloud(node_count, base_port=54321, ports_per_node=3, **kwargs):
    node_list = []
    try:
        for i in xrange(node_count):
            n = LocalH2O(port=base_port + i*ports_per_node, **kwargs)
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
    def __url(self, loc, port=None):
        if port is None: port = self.port
        return 'http://%s:%d/%s' % (self.addr, port, loc)

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

    def put_fileX(self, f, key=None, repl=None):
        resp1 =  self.__check_request(
            requests.get(self.__url('PutFile.json'), 
                params={"Key": key, "RF": repl} # key is optional. so is repl factor (called RF)
                ))
        verboseprint("put_file #1 phase: ", resp1)

        resp2 = self.__check_request(
            requests.put(self.__url('', port=resp1['port']), 
                files={"File": open(f, 'rb')}
                ))
        verboseprint("putf_file #2 phase: ", resp2)

        return resp2


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
    # xval gives us cross validation and more info
    def GLM(self, key, X="0", Y="1", family="binomial", xval=10):
        a = self.__check_request(requests.get(self.__url('GLM.json'),
            params={
                "family": family,
                "X": X,
                "Y": Y,
                "Key": key,
                "xval": xval
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
        args = [ 'java' ]
        if self.use_debugger:
            args += ['-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000']
        args += [
            "-ea", "-jar", self.get_h2o_jar(),
            "--port=%d" % self.port,
            '--ip=%s' % self.addr,
            '--ice_root=%s' % self.get_ice_dir(),
            '--name=pytest-%s' % getpass.getuser(),
            ]
        if not self.sigar:
            args += ['--nosigar']
        return args

    def __init__(self, addr=None, port=54321, capture_output=True, sigar=False, use_debugger=False):
        self.port = port
        self.addr = addr or get_ip_address()
        self.sigar = sigar
        self.use_debugger = use_debugger
        self.capture_output = capture_output

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
    def __init__(self, *args, **keywords):
        super(ExternalH2O, self).__init__(*args, **keywords)

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
    def __init__(self, *args, **keywords):
        super(LocalH2O, self).__init__(*args, **keywords)
        self.rc = None
        self.ice = tmp_dir('ice.')
        spawn = spawn_cmd('local-h2o', self.get_args(),
                capture_output=self.capture_output)
        self.ps = spawn[0]

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

class RemoteHost(object):
    def upload_file(self, f):
        f = find_file(f)
        if f not in self.uploaded:
            import md5
            m = md5.new()
            m.update(open(f).read())
            m.update(getpass.getuser())
            dest = '/tmp/' +m.hexdigest() +"-"+ os.path.basename(f)
            log('Uploading to %s: %s -> %s' % (self.addr, f, dest))
            sftp = self.ssh.open_sftp()
            sftp.put(f, dest)
            sftp.close()
            self.uploaded[f] = dest
        return self.uploaded[f]

    def __init__(self, addr, username, password=None):
        import paramiko
        self.addr = addr
        self.username = username
        self.ssh = paramiko.SSHClient()

        # don't require keys. If no password, assume passwordless setup was done
        policy = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(policy)
        self.ssh.load_system_host_keys()
        if password is None:
            self.ssh.connect(self.addr, username=username)
        else:
            self.ssh.connect(self.addr, username=username, password=password)

        self.uploaded = {}

    def remote_h2o(self, *args, **keywords):
        return RemoteH2O(self, self.addr, *args, **keywords)

    def open_channel(self):
        ch = self.ssh.get_transport().open_session()
        ch.get_pty() # force the process to die without the connection
        return ch

    def __str__(self):
        return 'ssh://%s@%s' % (self.username, self.addr)


class RemoteH2O(H2O):
    '''An H2O instance launched by the python framework on a remote machine'''
    def __init__(self, host, *args, **keywords):
        super(RemoteH2O, self).__init__(*args, **keywords)

        self.jar = host.upload_file(find_file('build/h2o.jar'))
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
    
