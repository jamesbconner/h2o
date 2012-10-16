import unittest
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_1m_rf(self):
        csvPathname = '../smalldata/test/arit.csv'
        h2o_cmd.runRF(trees=10, timeoutSecs=10, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main()
