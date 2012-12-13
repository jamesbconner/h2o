import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd as cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def notest_RF_iris2(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = h2o.find_file('smalldata/iris/iris2.csv')
        cmd.runRF(trees=trees, timeoutSecs=timeoutSecs, csvPathname=csvPathname)

    def notest_RF_poker100(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = h2o.find_file('smalldata/poker/poker100')
        cmd.runRF(trees=trees, timeoutSecs=timeoutSecs, csvPathname=csvPathname)

    def test_GenParity1(self):
        global SYNDATASETS_DIR
        global SYNSCRIPTS_DIR

        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # always match the run below!
        for x in xrange (2,100,10):
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split())
            # algorithm for creating the path and filename is hardwired in parity.pl.
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        trees = 6
        timeoutSecs = 20
        # always match the gen above!
        for x in xrange (2,100,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            cmd.runRF(trees=trees, timeoutSecs=timeoutSecs, csvPathname=csvPathname)

            trees += 10
            timeoutSecs += 2

if __name__ == '__main__':
    h2o.unit_main()
