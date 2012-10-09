import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_RFhhp(self):
        # changed to load the .gz directly
        # don't ever gunzip, since we want to test H2O's ability to use *.gz
        # if someone gunzip's it to look at it and the .gz goes away, they'll have to 
        # fix that problem in their directory (use git checkout <file>)
        csvPathnamegz = '../smalldata/hhp.cut3.214.data.gz'

        if not os.path.exists(csvPathnamegz):
            raise Exception("Can't find %s.gz" % (csvPathnamegz))

        print "RF start on ", csvPathnamegz, "this will probably take 10 minutes.."
        start = time.time()
        h2o_cmd.runRF(csvPathname=csvPathnamegz, trees=200,
                timeoutSecs=400, retryDelaySecs=15)
        print "RF end on ", csvPathnamegz, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
