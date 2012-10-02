import os, json, unittest, time, shutil, sys
import h2o

class SigarApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        global nodes
        nodes = h2o.build_cloud(node_count=3,sigar=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_netstat(self):
        # Ask each node for network statistics
        for n in nodes:
            a = n.netstat()
            print a

if __name__ == '__main__':
    unittest.main()
