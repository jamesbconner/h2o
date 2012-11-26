import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RFhhp(self):
        csvPathnamegz = '../smalldata/hhp_9_17_12.predict.100rows.data.gz'
        if not os.path.exists(csvPathnamegz):
            raise Exception("Can't find %s.gz" % (csvPathnamegz))

        h2o_cmd.runRF(trees=6, timeoutSecs=10, csvPathname=csvPathnamegz)

if __name__ == '__main__':
    h2o.unit_main()
