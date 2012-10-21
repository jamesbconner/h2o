

# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser

import unittest
import h2o_cmd, h2o, h2o_hosts
import time
import webbrowser
class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Uses your username specific json: pytest_config-<username>.json

        # do what my json says, but with my hdfs. hdfs_name_node from the json
        # I'll set use_hdfs to False here, because H2O won't start if it can't talk to the hdfs
        h2o_hosts.build_cloud_with_hosts(use_hdfs=False)
        # h2o_hosts.build_cloud_with_hosts(use_hdfs=True, hdfs_name_node="192.168.0.37")

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_1m_rf_w_browser(self):
        # after cloud building, node[0] should have the right info for us
        url = "http://" + h2o.nodes[0].addr + ":" + str(h2o.nodes[0].port)

        # FIX! I guess we could just open up a bunch of tabs to different urls
        # Open URL in a new tab, if a browser window is already open.
        webbrowser.open_new_tab(url)
        # Open URL in new window, raising the window if possible.
        webbrowser.open_new(url)

        csvPathname = '../smalldata/poker/poker1000'
        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)

        # hang for an hour, so you can play with the browser
        # FIX!, should be able to do something that waits till browser is quit?
        time.sleep(600)

if __name__ == '__main__':

    h2o.unit_main()

