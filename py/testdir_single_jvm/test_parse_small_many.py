import unittest
import re, os, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

def writeRows(csvPathname,row,eol,repeat):
    f = open(csvPathname, 'w')
    for r in range(repeat):
        f.write(row + eol)

def testit():
    SEED = 6204672511291494176
    random.seed(SEED)
    print "\nUsing random seed:", SEED

    h2o.build_cloud(1)
    SYNDATASETS_DIR = h2o.make_syn_dir()
    # can try the other two possibilities also
    eol = "\n"
    # row = "0,1,2,3,4,5,6,7"
    row = "a,b,c,d,e,f,g"

    # need unique key name for upload and for parse, each time
    # maybe just upload it once?
    timeoutSecs = 10
    node = h2o.nodes[0]

    # fail rate is one in 200?
    # need at least two rows (parser)
    for sizeTrial in range(7):
        size = random.randint(2,129)
        print "\nparsing with rows:", size
        csvFilename = "p" + "_" + str(size)
        csvPathname = SYNDATASETS_DIR + "/" + csvFilename
        writeRows(csvPathname,row,eol,size)
        key = csvFilename
        put = node.put_file(csvPathname, key=key, timeoutSecs=timeoutSecs)
        for trial in range(5):
            key2 = csvFilename + "_" + str(trial) + ".hex"
            # just parse
            node.parse(put['key'], key2, timeoutSecs=timeoutSecs, retryDelaySecs=0.00)
            sys.stdout.write('.')
            sys.stdout.flush()

    h2o.tear_down_cloud()

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # SEED = random.randint(0, sys.maxint)
        # SEED = 2601342207789799765
        # SEED = 7200968798652201060
        pass

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()

    def test_A_parse_small_many(self):
        testit()

    def test_B_parse_small_many(self):
        testit()

    def test_C_parse_small_many(self):
        testit()

    def test_D_parse_small_many(self):
        testit()

    def test_E_parse_small_many(self):
        testit()

    def test_F_parse_small_many(self):
        testit()

    def test_G_parse_small_many(self):
        testit()

if __name__ == '__main__':
    h2o.unit_main()
