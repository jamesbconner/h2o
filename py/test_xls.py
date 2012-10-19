import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class TestExcel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_iris_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xls'))

    def test_iris_xlsx(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xlsx'))

    def test_poker_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('poker/poker-hand-testing.xls'))

    def test_poker_xlsx(self):
        h2o_cmd.runRF(None, h2o.find_dataset('poker/poker-hand-testing.xlsx'))

if __name__ == '__main__':
    h2o.unit_main()
