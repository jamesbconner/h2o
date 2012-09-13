import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc

# for the ip address detection
import commands
import socket

def addNode():
    global nodes

    # Hackery: find the ip address that gets you to Google's DNS
    # Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
    # Will fail if local proxy? we don't have one.
    # Watch out to see if there are NAT issues here (home router?)
    # Could parse ifconfig, but would need something else on windows
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    ipaddr = s.getsockname()[0]
    if (ipaddr.startswith('127')):
        ipAddress = "127.0.0.1"
        print "Can't figure it out: Using", ipAddress
    else:
        ipAddress = ipaddr
        print ipAddress, "- Using this IP from socket.gethostname"

        
    portForH2O = 54321
    portsPerNode = 3
    print "Assuming H2O ports start at", portForH2O, "with", portsPerNode, "ports per node"
    h = h2o.H2O(ipAddress, portForH2O + len(nodes)*portsPerNode)
    ## print h
    nodes.append(h)
    print "H2O Node count:", len(nodes)

def runRF(n,trees):
    put = n.put_file('../smalldata/iris/iris2.csv')
    # FIX! temp hack to avoid races?
    time.sleep(0.5)
    parse = n.parse(put['keyHref'])
    # FIX! temp hack to avoid races?
    time.sleep(0.5)
    rf = n.random_forest(parse['keyHref'],trees)
    # FIX! temp hack to avoid races?
    time.sleep(0.5)
    # this expenses the response to match the number of trees you told it to do
    n.stabilize('random forest finishing', 20,
        lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == trees)

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        try:
            proc.clean_sandbox()
            nodes = []

            for i in range(3):
                addNode()

            # give them a few seconds to stabilize
            nodes[0].stabilize('cloud auto detect', len(nodes),
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
