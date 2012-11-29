import unittest
import h2o, h2o_cmd
import re, os, shutil

# test some random csv data, and some lineend combinations
class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

        # Create a directory for the created dataset files. ok if already exists
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_randomdata2(self):
        csvPathname = '../smalldata/datagen1.csv'
        h2o_cmd.runRF(trees=37, timeoutSecs=10, csvPathname=csvPathname)

    def test_B_randomdata2_1_lineend(self):
        # change lineend, case 1
        infile = open("../smalldata/datagen1.csv", 'r') 
        csvPathname = SYNDATASETS_DIR + "/datagen1_1.csv"
        outfile = open(csvPathname,'w') # existing file gets erased

        # assume all the test files are unix lineend. 
        # I guess there shouldn't be any "in-between" ones
        # okay if they change I guess.
        for line in infile.readlines():
            outfile.write(line.strip("\n") + "\r")

        h2o_cmd.runRF(trees=39, timeoutSecs=10, csvPathname=csvPathname)
        infile.close()
        outfile.close()
        h2o_cmd.runRF(trees=7, timeoutSecs=10, csvPathname=csvPathname)


if __name__ == '__main__':
    h2o.unit_main()
