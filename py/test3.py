import os, json, unittest, time, shutil, sys
import util.h2o as h2o

# a key gets generated afte a put.
def putFile(n,csvPathname):
    put = n.put_file(csvPathname)
    parseKey = n.parse(put['keyHref'])

    ### print 'After put, parseKey:', parseKey
    ## print 'key', parseKey['key']
    ## print 'keyHref', parseKey['keyHref']
    ## print 'put TimeMS', parseKey['TimeMS']

    # ?? how we we check that the put completed okay?
    return parseKey

# we pass the key from the put, for knowing what to RF on.
def runRFonly(n,parseKey,trees,depth,timeoutSecs):
    # FIX! what is rf set to here (on pass/fail?)
    rf = n.random_forest(parseKey['keyHref'],trees,depth)
    ### print 'After random_forest, rf:', rf
    ## print 'confKey', rf['confKey']
    ## print 'confKeyHref', rf['confKeyHref']

    # has the ip address for the cloud
    ## print 'h2o', rf['h2o']
    ## print 'depth', rf['depth'] # depth seems to change correctly now?

    # this expects the response to match the number of trees you told it to do
    n.stabilize('random forest finishing', timeoutSecs,
        lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == trees)

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

    # NOTE: unittest will run tests in an arbitrary order..not constrained to order here.

    # Possible hack: change the names so this test order matches alphabetical order
    # by using intermediate "_A_" etc. 
    # That should make unittest order match order here? 

    def test_Basic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

    def test_GenParity1(self):
        global SYNDATASETS_DIR
        global SYNSCRIPTS_DIR

        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # always match the run below!
        for x in [10000]:
            # Have to split the string out to list for pipe
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd('parity.pl', shCmdString.split())
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # FIX! I suppose we should vary the number of trees to make sure the response changes
        # maybe just inc in loop
        trees = 137 # 200ms?
        depth = 45
        # bump this up too if you do?
        timeoutSecs = 10

        # always match the gen above!
        trial = 1

        print "This currently hangs/fails after ? trials or so. stdout/stderr all seem good"
        for x in xrange (10000,20000,50):
            sys.stdout.write('.')
            sys.stdout.flush()

            # just use one file for now
            csvFilename = "parity_128_4_" + str(10000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            # FIX! TBD do we always have to kick off the run from node 0?

            # CNC - My antique computer reports files missing without a little delay here.
            time.sleep(0.1)
            # broke out the put separately so we can iterate a test just on the RF
            parseKey = putFile(nodes[0],csvPathname)

            print 'Trial:', trial
            ### print 'put TimeMS:', parseKey['TimeMS']

            runRFonly(nodes[0],parseKey,trees,depth,timeoutSecs)
            ### print "Trial", trial, "done"

            # don't change tree count yet
            ## trees += 10
            ### timeoutSecs += 2
            trial += 1



if __name__ == '__main__':
    unittest.main()
