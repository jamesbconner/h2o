import unittest
import h2o, h2o_cmd

class TestKaggle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_cs_training(self):
        h2o_cmd.runRF(trees=10, timeoutSecs=10, csvPathname=h2o.find_file('smalldata/kaggle/creditsample-training.csv.gz'))

    def test_cs_test(self):
        h2o_cmd.runRF(trees=10, timeoutSecs=10, csvPathname=h2o.find_file('smalldata/kaggle/creditsample-training.csv.gz'))

if __name__ == '__main__':
    h2o.unit_main()
