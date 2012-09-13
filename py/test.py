import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc
import os
import shutil

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

def runRF(n,trees,csvPathname,timeoutSecs):
    put = n.put_file(csvPathname)
    parse = n.parse(put['keyHref'])
    time.sleep(0.5) # FIX! temp hack to avoid races?

    rf = n.random_forest(parse['keyHref'],trees)
    # this expects the response to match the number of trees you told it to do
    n.stabilize('random forest finishing', timeoutSecs,
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

    def testRFiris2(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = '../smalldata/iris/iris2.csv'

        # FIX! TBD do we always have to kick off the run from node 0?
        # what if we do another node?
        runRF(nodes[0],trees,csvPathname,timeoutSecs)

    def testParityGen(self):
        # FIX! TBD Matt suggests that devs be required to git pull "datasets"next to hexbase..
        # so we can get files from there, without generating datasets

        # FIX! TBD Matt suggests having a requirement for devs to test with HDFS
        # Can talk to HDFS with the right args to H2O initiation? (e.g. hduser for one)

        # Create a directory for the created dataset files. ok if already exists
        global SYNDATASETS_DIR
        global SYNSCRIPTS_DIR

        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # Trying a possible strategy for creating tests on the fly.
        # Creates the filename from the args, in the right place
        #   i.e. ./syn_datasets/parity_128_4_1024_quad.data
        # The .pl assumes ./syn_datasets exists.
    
        # first time we use perl
        # get something known piped to standbox/*stdout, 
        # until we know everyone has perl setup right
        # (windows/mac/centos/ubuntu/virtualbox)
        pipe = proc.Process(["/usr/bin/perl", "-h"])

        # Have to split the string out to list for pipe
        shCmdString = SYNSCRIPTS_DIR + "/parity.pl 128 4 1024 quad"
        shCmdList =  shCmdString.split()
        pipe = proc.Process(shCmdList)

    def testRFparity1(self):
        # Assuming the tests are run in order, we can assume the 
        # dataset is available now? maybe not true if this test is run as one off?
        global SYNDATASETS_DIR
        trees = 6
        timeoutSecs = 20
        csvPathname = SYNDATASETS_DIR + '/parity_128_4_1024_quad.data'

        # FIX! TBD do we always have to kick off the run from node 0?
        # what if we do another node?
        runRF(nodes[0],trees,csvPathname,timeoutSecs)


if __name__ == '__main__':
    unittest.main()
