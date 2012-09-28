import os, json, unittest, time, shutil, sys
import h2o, cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_RFhhp(self):
        # changed to load the .gz directly
        # don't every gunzip, since we want to test H2O's ability to use *.gz
        # if someone gunzip's it to look at it and the .gz goes away, they'll have to 
        # fix that problem in their directory (use git checkout <file>)
        csvPathnamegz = '../smalldata/hhp_9_17_12.predict.100rows.data.gz'

        if not os.path.exists(csvPathnamegz):
            raise Exception("Can't find %s.gz" % (csvPathnamegz))

        cmd.runRF(trees=6, timeoutSecs=10, csvPathname=csvPathnamegz)

if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
