import time, os, json, signal, tempfile, shutil, datetime
import requests
import psutil

def find_file(base):
    f = base
    if not os.path.exists(f): f = '../'+base
    return f

LOG_DIR = 'sandbox'
def clean_sandbox():
    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR)
    os.mkdir(LOG_DIR)

def log_file(f):
    return tempfile.mkstemp(prefix=f, suffix='.log', dir=LOG_DIR)

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
    except:
        pass
    if ip.startswith('127'):
        ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]
    return ip

def spawn_cmd(name, args):
    outfd,outpath = log_file(name + '.stdout.')
    errfd,errpath = log_file(name + '.stderr.')
    ps = psutil.Popen(args, stdin=None, stdout=outfd, stderr=errfd)
    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    log(' '.join(args), comment=comment)
    return (ps, outpath, errpath)

def tear_down_cloud(nodes):
    ex = None
    for n in nodes:
        if n.wait() is None:
            n.terminate()
        elif n.wait():
            ex = Exception('Node terminated with non-zero exit code: %d' % n.wait())
    if ex: raise ex

def build_cloud(node_count, base_port=54321, ports_per_node=3):
    nodes = []
    try:
        for i in xrange(node_count):
            n = H2O(port=base_port + i*ports_per_node)
            nodes.append(n)
        nodes[0].stabilize('cloud auto detect', 2,
            lambda n: n.get_cloud()['cloud_size'] == len(nodes))
    except:
        for n in nodes: n.terminate()
        raise
    return nodes

class H2O:
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __check_request(self, r):
        log('Sent ' + r.url)
        if not r:
            import inspect
            raise Exception('Error in %s: %s' % (inspect.stack()[1][3], str(r)))
        return r.json

    def __check_spawn(self):
        if not self.ps:
            import inspect
            raise Exception('Error in %s: %s' % (inspect.stack()[1][3], 'process was not spawned'))

    def get_cloud(self):
        return self.__check_request(requests.get(self.__url('Cloud.json')))

    def put_file(self, f):
        return self.__check_request(requests.post(self.__url('PutFile.json'), 
            files={"File": open(f, 'rb')}))

    def parse(self, key):
        return self.__check_request(requests.get(self.__url('Parse.json'),
            params={"Key": key}))

    def random_forest(self, key, ntrees):
        return self.__check_request(requests.get(self.__url('RF.json'),
            params={
                "ntrees": ntrees,
                "Key": key
                }))

    def random_forest_view(self, key):
        return self.__check_request(requests.get(self.__url('RFView.json'),
            params={"Key": key}))

    def stabilize(self, msg, timeout, func):
        start = time.time()
        while time.time() - start < timeout:
            if func(self):
                break
            time.sleep(0.1)
        else:
            raise Exception('Timeout waiting for condition: ' + msg)

    def __is_alive(self, s2):
        assert self == s2
        try:
            self.get_cloud()
            return True
        except requests.ConnectionError, e:
            if e.args[0].errno == 61 or e.args[0].errno == 111:
                return False
            raise

    def __init__(self, addr=None, port=54321, spawn=True):
        self.port = port
        self.addr = addr or get_ip_address()
        if not spawn:
            self.stabilize('h2o started', 2, self.__is_alive)
        else:
            self.rc = None
            spawn = spawn_cmd('h2o', [
                    "java", "-ea", "-jar", find_file('build/h2o.jar'),
                    "--port=%d"%self.port,
                    '--ip=%s'%self.addr,
                    '--nosigar',
                    '--ice_root=%s' % tempfile.mkdtemp(prefix='ice',dir=LOG_DIR)
            ])
            self.ps = spawn[0]
            try:
                self.stabilize('h2o started', 2, self.__is_alive)
            except:
                self.ps.kill()
                raise

            time.sleep(1)
            if self.wait():
                out = file(spawn[1]).read()
                err = file(spawn[2]).read()
                raise Exception('Failed to launch with exit code: %d\nstdout:\n%s\n\nstderr:\n%s' % 
                    (self.wait(), out, err))

    def stack_dump(self):
        self.__check_spawn()
        self.ps.send_signal(signal.SIGQUIT)
    
    def wait(self, timeout=0):
        self.__check_spawn()
        if self.rc: return self.rc
        try:
            self.rc = self.ps.wait(timeout)
            return self.rc
        except psutil.TimeoutExpired:
            return None

    def terminate(self):
        self.__check_spawn()
        return self.ps.kill()
