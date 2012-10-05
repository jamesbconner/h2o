import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        #pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_A_putfile(self):
        cvsfile = h2o.find_file('smalldata/poker/poker100')
        node = h2o.nodes[0]
        node.put_fileX(cvsfile)
# TODO check response and returned size if it match with the size of cvsfile

#    def test_B_large_putfile(self):
#        cvsfile = h2o.find_file('/tmp/Rango.avi')
#        node = h2o.nodes[0]
#        node.put_fileX(cvsfile)


if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
