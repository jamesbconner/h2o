import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
# Will fail if local proxy? we don't have one.
# Watch out to see if there are NAT issues here (home router?)
# Could parse ifconfig, but would need something else on windows
def getIpAddress():
    import socket
    ip = '127.0.0.1'
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))
        ip = s.getsockname()[0]
    except:
        pass
    if ip.startswith('127'):
        ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]
    return ip
    
ipAddress = getIpAddress()

def addNode():
    global nodes
    portForH2O = 54321
    portsPerNode = 3
    h = h2o.H2O(ipAddress, portForH2O + len(nodes)*portsPerNode)
    nodes.append(h)

def runRF(n,trees):
    put = n.put_file('../smalldata/iris/iris2.csv')
    parse = n.parse(put['keyHref'])
    time.sleep(0.5) # FIX! temp hack to avoid races?
    rf = n.random_forest(parse['keyHref'],trees)
    # this expects the response to match the number of trees you told it to do
    n.stabilize('random forest finishing', 20,
        lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == trees)

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        try:
            proc.clean_sandbox()
            nodes = []
            for i in range(3): addNode()
            # give them a few seconds to stabilize
            nodes[0].stabilize('cloud auto detect', 2,
                lambda n: n.get_cloud()['cloud_size'] == len(nodes))
        except:
            for n in nodes: n.terminate()
            raise

    @classmethod
    def tearDownClass(cls):
        ex = None
        for n in nodes:
            if n.wait() is None:
                n.terminate()
            elif n.wait():
                ex = Exception('Node terminated with non-zero exit code: %d' % n.wait())
        if ex: raise ex

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBasic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

    def testRF(self):
        trees = 6
        runRF(nodes[0],trees)

if __name__ == '__main__':
    unittest.main()
