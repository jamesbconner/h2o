import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global nodes
        nodes = h2o.build_cloud(node_count=3)

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
        result = node.put_fileX(cvsfile)

        origSize   = h2o.get_file_size(cvsfile)
        returnSize = result[0]['size']
        self.assertEqual(origSize,returnSize)
    
    def test_B_putfile(self):

        cvsfile = h2o.find_file('smalldata/poker/poker1000')
        origSize   = h2o.get_file_size(cvsfile)
        for node in nodes:
            result = node.put_fileX(cvsfile)

            returnSize = result[0]['size']
            self.assertEqual(origSize,returnSize)


#    def test_B_large_putfile(self):
#        cvsfile = h2o.find_file('/tmp/Rango.avi')
#        node = h2o.nodes[0]
#        node.put_fileX(cvsfile)


if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
