import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_B_GenParity1(self):
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

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        for x in xrange (1,30,1):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvPathname = "../smalldata/hhp_107_01_short.data"
            put = n.put_file(csvPathname)
            parseKey = n.parse(put['keyHref'])

            ### print 'Trial:', trial
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
