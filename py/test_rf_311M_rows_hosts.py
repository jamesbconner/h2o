import unittest
import h2o_cmd, h2o, h2o_hosts
import webbrowser

# Uses your username specific json: pytest_config-<username>.json
# copy pytest_config-simple.json and modify to your needs.
class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_311M_rf(self):
        # since we'll be waiting, pop a browser
        url = "http://" + h2o.nodes[0].addr + ":" + str(h2o.nodes[0].port)
        webbrowser.open_new(url)

        # just my file

        csvPathname = './new-poker-hand.full.311M.txt.gz'
        # broke out the put separately so we can iterate a test just on the RF
        # FIX! trying node 1..0 was failing?
	parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)
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


