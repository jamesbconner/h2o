import unittest, os
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_claim_prediction(self):
        csvPathname = '../smalldata/allstate/claim_prediction_train_set_10000_int.csv.gz'

        if not os.path.exists(csvPathname):
            raise Exception("Can't find %s." % (csvPathname))

        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main() 
