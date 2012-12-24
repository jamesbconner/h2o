import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd as cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_LR_iris2(self):
        timeoutSecs = 2
        csvPathname = h2o.find_file('smalldata/iris/iris2.csv')
        colA=0
        colB=1
        cmd.runLR(csvPathname=csvPathname,colA=colA,colB=colB,
            timeoutSecs=timeoutSecs)

    def test_C_LR_poker1000(self):
        timeoutSecs = 2
        csvPathname = h2o.find_file('smalldata/poker/poker1000')
        colA=0
        colB=1
        cmd.runLR(csvPathname=csvPathname,colA=colA,colB=colB,
            timeoutSecs=timeoutSecs)

    def test_D_GenParity1(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        parityPl = h2o.find_file('syn_scripts/parity.pl')
        # always match the run below!
        for x in range(91,92):
            shCmdString = "perl " + parityPl + " 128 4 "+ str(x) + " quad"
            print shCmdString

            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split())
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        timeoutSecs = 2
        for colA in xrange (0,9,1):
            for colB in xrange (colA,9,1):
                sys.stdout.write('.')
                sys.stdout.flush()
                csvFilename = "parity_128_4_" + str(91) + "_quad.data"  
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                cmd.runLR(csvPathname=csvPathname,colA=colA,colB=colB,
                    timeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
