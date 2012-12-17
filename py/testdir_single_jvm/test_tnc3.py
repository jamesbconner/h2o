import unittest
import time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_browse as h2b

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_tnc3_ignore(self):
        csvPathname = h2o.find_file('smalldata/tnc3.csv')
        h2b.browseTheCloud()

        print "\nWe're not CM data getting back from RFView.json that we can check!. so look at the browser"
        print 'The good case with ignore="boat,body"'
        rfv = h2o_cmd.runRF(trees=5, timeoutSecs=10, ignore="boat,body", csvPathname=csvPathname)
        print "RFView.json result:"
        print h2o.dump_json(rfv)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")
        # warning: can't use the browser to go back and look at individual trees..they will be the next trees
        # if we don't wait here
        # time.sleep(1500)

        print "\nNow the bad case (no ignore)"
        rfv = h2o_cmd.runRF(trees=5, timeoutSecs=10, csvPathname=csvPathname)
        print "RFView.json result:"
        print h2o.dump_json(rfv)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        print "\n <ctrl-C> to quit sleeping here"
        if not h2o.browse_disable:
            time.sleep(1500)

if __name__ == '__main__':
    h2o.unit_main()
