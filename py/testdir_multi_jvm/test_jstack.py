import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o

class JStackApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        global nodes
        nodes = h2o.build_cloud(3,sigar=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def test_jstack(self):
        # Ask each node for jstack statistics
        for n in nodes:
            stats = n.jstack()
            h2o.verboseprint(json.dumps(stats,indent=2))

if __name__ == '__main__':
    h2o.unit_main()
