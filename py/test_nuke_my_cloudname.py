import os, json, unittest, time, shutil, sys
import h2o_cmd, h2o

# This guy doesn't minimal checking. Starts one local node, and hopefully zombies with same name
# connect with it. (hopefully your local node is talking to the network so any remotely started zombies
# can interact with it

class Basic(unittest.TestCase):
    def test_Nuke(self):
        h2o.build_cloud(node_count=1)
        # wait 10 seconds for zombies to latch on?
        print "Waiting 10 secs for unknown number of possible zombies"
        time.sleep(10)

        c = h2o.nodes[0].get_cloud()
        cloudSize = c['cloud_size']
        print "This node thought there was", cloudSize, "nodes in its cloud"

        # FIX! I added shutdown_all to LocalH2O so maybe this is now redundant
        sd = h2o.nodes[0].shutdown_all()

        # this doesn't send a graceful shutdown? but should be tolerant of missing process?
        h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.unit_main()
