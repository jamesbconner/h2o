import os, json, unittest, time, shutil, sys
import h2o_cmd
import h2o
import h2o_browse as h2b

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    # NOTE: unittest will run tests in an arbitrary order..not constrained to order written here.
    # by using intermediate "_A_" etc. That should make unittest order match order here? 

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_B_RF_iris2(self):
        RFview = h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/iris/iris2.csv'))

    def test_C_RF_poker100(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/poker/poker100'))

    def test_D_GenParity1(self):
        # Create a directory for the created dataset files. ok if already exists
        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # create datasets with a perl 
        # Creates the filename from the args, in the right place
        #   i.e. ./syn_datasets/parity_128_4_1024_quad.data
        # The .pl assumes ./syn_datasets exists.
        # first time we use perl (parity.pl)

        # always match the run below!
        for x in xrange (11,100,10):
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            # FIX! as long as we're doing a couple, you'd think we wouldn't have to 
            # wait for the last one to be gen'ed here before we start the first below.
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=3)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        trees = 6
        timeoutSecs = 20

        # always match the gen above!
        if (h2o.browse_json):
            h2b.browseTheCloud()
        for x in xrange (11,60,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            # Only pop the browsers down in RF if verbose!
            h2o_cmd.runRF(trees=trees, timeoutSecs=timeoutSecs, csvPathname=csvPathname, browseAlso=h2o.browse_json)
            trees += 10

if __name__ == '__main__':
    h2o.unit_main()
