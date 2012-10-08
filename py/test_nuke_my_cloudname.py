import os, json, unittest, time, shutil, sys
import h2o_cmd, h2o

# This guy doesn't minimal checking. Starts one local node, and hopefully zombies with same name
# connect with it. (hopefully your local node is talking to the network so any remotely started zombies
# can interact with it

class Basic(unittest.TestCase):
    def test_Nuke(self):
        h2o.build_cloud(node_count=1)
        h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.unit_main()
