import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class TestExcel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    # try a lot of trees
    def test_iris_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xls'), trees=100)

    def test_iris_xlsx(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xlsx'), trees=100)

    # FIX! why are these timing out with smaller numbers? do we need bigger defaults in runRF?
    def test_poker_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('poker/poker-hand-testing.xls'), trees=51, timeoutSecs=60)

    def test_poker_xlsx(self):
        h2o_cmd.runRF(None, h2o.find_dataset('poker/poker-hand-testing.xlsx'), trees=51, timeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
