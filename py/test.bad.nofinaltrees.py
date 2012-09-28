import os, json, unittest, time, shutil, sys
import h2o

def runRF(n,trees,csvPathname,timeoutSecs):
    put = n.put_file(csvPathname)
    parse = n.parse(put['keyHref'])
    rf = n.random_forest(parse['keyHref'],trees)
    # kbn hack to make test.py pass reliably
    # time.sleep(1)
    # this expects the response to match the number of trees you told it to do
    print "retryDelaySecs = 0.10 after RF"
    n.stabilize('random forest finishing', timeoutSecs,
        lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == trees,
        retryDelaySecs=0.10)

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        global nodes
        nodes = h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # NOTE: unittest will run tests in an arbitrary order..not constrained
    # to order written here.

    # should change the names so this test order matches alphabetical order
    # by using intermediate "_A_" etc. That should make unittest order match
    # order here? 

    def test_D_GenParity1(self):
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
        for x in xrange (50,200,10):
            # Have to split the string out to list for pipe
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            # FIX! as long as we're doing a couple, you'd think we wouldn't have to 
            # wait for the last one to be gen'ed here before we start the first below.
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=3)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # FIX! I suppose we should vary the number of trees to make sure the response changes
        # maybe just inc in loop
        trees = 100
        # bump this up too if you do?
        timeoutSecs = 5 
        # always match the gen above!
        ### for x in xrange (50,200,10):
        for x in xrange(50,200,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            # csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvFilename = "parity_128_4_" + "100" + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "\n", csvPathname, "trees:", trees, "timeoutSecs", timeoutSecs
            # FIX! TBD do we always have to kick off the run from node 0?
            # what if we do another node?
            # FIX! do we need or want a random delay here?
            # CNC - My antique computer reports files missing without a little delay here.
            runRF(nodes[0],trees,csvPathname,timeoutSecs)

            ### trees += 10
            ## hitimeoutSecs += 2


if __name__ == '__main__':
    unittest.main()
