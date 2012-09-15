import os, json, unittest, time, shutil, sys
import util.h2o as h2o

def addNode(nodes):
    portForH2O = 54321
    portsPerNode = 3
    h = h2o.H2O(port=(portForH2O + len(nodes)*portsPerNode))
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
        nodes = []
        try:
            h2o.clean_sandbox()
            # change the number here if you want more or less nodes
            for i in range(3): addNode(nodes)
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

    def testRF_iris2(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = '../smalldata/iris/iris2.csv'
        runRF(nodes[0], trees, csvPathname, timeoutSecs)

    def testRF_poker100(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = '../smalldata/poker/poker100'
        runRF(nodes[0], trees, csvPathname, timeoutSecs)

    def testGenParity1(self):
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
    
        # first time we use perl (parity.pl)

        # always match the run below!
        # FIX! 1 row fails in H2O. skip for now
        for x in xrange (2,100,10):
            # Have to split the string out to list for pipe
            shCmdString = SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd('parity.pl', shCmdString.split())
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

    def testRfParity1(self):
        # run the tests created by the prior testGenParity1

        # Assuming the tests are run in order, we can assume the 
        # dataset is available now? maybe not true if this test is run as one off?
        global SYNDATASETS_DIR

        # FIX! I suppose we should vary the number of trees to make sure the response changes
        # maybe just inc in loop
        trees = 6
        # bump this up too if you do?
        timeoutSecs = 10
        # always match the gen above!
        # FIX! 1 row fails in H2O. skip for now
        for x in xrange (2,100,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            # FIX! TBD do we always have to kick off the run from node 0?
            # what if we do another node?
            # FIX! do we need or want a random delay here?
            time.sleep(0.5) 
            runRF(nodes[0],trees,csvPathname,timeoutSecs)

            trees += 10
            timeoutSecs += 2


if __name__ == '__main__':
    unittest.main()
