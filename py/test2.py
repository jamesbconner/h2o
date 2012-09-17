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
        # so using .zip for now. load .zip direct
        # I unzip below..good thing to try? may distribute larger .zip's in hexbase/smalldata

        # This is just a history of files I was trying..keep it here so I remember to try
        # them later
        ### csvPathname = '../smalldata/hhp_9_14_12.data'

        # 0-32 output class values
        ### csvPathname = '../smalldata/hhp_50.short3.data'

        # 0-15 output class values
        csvPathname = '../smalldata/hhp_107_01_short.data'

        if not os.path.exists(csvPathname) :
            # check if the gz exists (git will have the .gz)
            if os.path.exists(csvPathname + '.zip') :
                # FIX! matt will probably say to use the other asyncproc to log things right?
                check_call(['unzip', csvPathname + '.zip'])
            else:
                raise Exception("Can't find %s or %s.zip" % csvPathname, csvPathname)

        # FIX! TBD do we always have to kick off the run from node 0?
        # what if we do another node?
        print "RF start on ", csvPathname
        runRF(nodes[0],trees,csvPathname,timeoutSecs)
        print "RF end on ", csvPathname

if __name__ == '__main__':
    unittest.main()
