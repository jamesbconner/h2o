import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd as cmd 
import argparse

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GenParity1(self):
        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # always match the run below!
        # just using one file for now
        for x in [10000]:
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        for trial in xrange (1,10,1):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvFilename = "parity_128_4_" + str(10000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            # broke out the put separately so we can iterate a test just on the RF
            put = h2o.nodes[0].put_file(csvPathname)
            parseKey = h2o.nodes[0].parse(put['key'])

            h2o.verboseprint("Trial", trial)
            cmd.runRFOnly(parseKey=parseKey, trees=10000, depth=100, timeoutSecs=300, retryDelaySecs=3)

if __name__ == '__main__':
    h2o.unit_main()

