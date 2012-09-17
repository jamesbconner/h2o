import os, json, unittest, time, shutil, sys
import util.h2o as h2o

def runRF(n,trees,csvPathname,timeoutSecs):
    put = n.put_file(csvPathname)
    parse = n.parse(put['keyHref'])
    time.sleep(0.5) # FIX! temp hack to avoid races?
    rf = n.random_forest(parse['keyHref'],trees)
    # this expects the response to match the number of trees you told it to do
    n.stabilize('random forest finishing', timeoutSecs,
        lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == trees)

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        global nodes
        nodes = h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_RFhhp(self):
        trees = 6
        timeoutSecs = 10

        # FIX! we're supposed to be support gzip files but seems to fail
        # normally we .gz for the git, to save space
        # we do take .zip directly. bug fix needed for .gz


        # 0-32 output class values
        ### csvPathname = '../smalldata/hhp_50.short3.data'

        # 0-15 output class values
        ## csvPathname = '../smalldata/hhp_107_01_short.data'

        # don't want to worry about timeout on RF, so just take the first 100 rows
        # csvPathname = '../smalldata/hhp_9_17_12.predict.data'
        csvPathname = '../smalldata/hhp_9_17_12.predict.100rows.data'

        if not os.path.exists(csvPathname) :
            # check if the gz exists (git will have the .gz)
            if os.path.exists(csvPathname + '.gz') :
                # keep the .gz in place
                h2o.spawn_cmd_and_wait(
                    'gunzip', 
                    ['gunzip', csvPathname + '.gz'], 
                    timeout=4)
            else:
                raise Exception("Can't find %s or %s.gz" % (csvPathname, csvPathname))

        # FIX! TBD do we always have to kick off the run from node 0?
        # what if we do another node?
        print "RF start on ", csvPathname
        runRF(nodes[0],trees,csvPathname,timeoutSecs)
        print "RF end on ", csvPathname

if __name__ == '__main__':
    unittest.main()
