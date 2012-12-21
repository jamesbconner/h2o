import unittest, sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b

# Uses your username specific json: pytest_config-<username>.json
# copy pytest_config-simple.json and modify to your needs.
class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_311M(self):
        # since we'll be waiting, pop a browser
        h2b.browseTheCloud()

        # just my file
        csvPathname = h2o.find_file('new-poker-hand.full.311M.txt.gz')
        # broke out the put separately so we can iterate a test just on the RF
        # FIX! trying node 1..0 was failing?
	parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=1000)
        print 'parse TimeMS:', parseKey['TimeMS']

        trials = 0
        trees = 4
        for trials in range(10):
            # h2o_cmd.runRF(trees=16, timeoutSecs=7200, csvPathname=csvPathname)
            h2o_cmd.runRFOnly(parseKey=parseKey, trees=trees, depth=15, timeoutSecs=7200)
            trials += 1
            print "Trial", trials
            trees += 3


if __name__ == '__main__':
    h2o.unit_main()


