import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd as cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global nodes
        nodes = h2o.build_cloud(node_count=1)

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

    def test_A_Basic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

    def test_C_GLM_poker1000(self):
        timeoutSecs = 2
        csvPathname = h2o.find_file('smalldata/poker/poker1000')
        X="9"
        Y="10"
        cmd.runGLM(csvPathname=csvPathname,X=X,Y=Y,timeoutSecs=timeoutSecs)

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
        for x in range(91,92):
            # Have to split the string out to list for pipe
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            print shCmdString

            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split())
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        timeoutSecs = 2
        
        Y = "10"
        X = "0"
        # 10 trials, each with one more X input
        for appendX in xrange (0,10,1):
            sys.stdout.write('.')
            sys.stdout.flush()
            if (appendX == 0): 
                X = "0"
            else:
                X = X + "," + str(appendX)

            print "X:", X
            csvFilename = "parity_128_4_" + str(91) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            cmd.runGLM(csvPathname=csvPathname,X=X,Y=Y,timeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
