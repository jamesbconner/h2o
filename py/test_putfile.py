import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import itertools

def chunked_file(file, chunk_size):
    return iter(lambda: file.read(chunk_size), '')

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

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
        node    = h2o.nodes[0]
        result  = node.put_fileX(cvsfile)

        origSize   = h2o.get_file_size(cvsfile)
        returnSize = result[0]['size']
        self.assertEqual(origSize,returnSize)
    
    def test_B_putfile_to_all_nodes(self):

        cvsfile  = h2o.find_file('smalldata/poker/poker1000')
        origSize = h2o.get_file_size(cvsfile)
        for node in h2o.nodes:
            result     = node.put_fileX(cvsfile)
            returnSize = result[0]['size']
            self.assertEqual(origSize,returnSize)
    
    def test_C_putfile_and_getfile(self):

        cvsfile = h2o.find_file('smalldata/poker/poker1000')
        node    = h2o.nodes[0]
        result  = node.put_fileX(cvsfile)

        key     = result[0]['keyHref']
        r       = node.get_key(key)
        f       = open(cvsfile)
        self.diff(r, f)
        f.close()

    def test_D_putfile_and_getfile_to_all_nodes(self):

        cvsfile = h2o.find_file('smalldata/poker/poker1000')
        for node in h2o.nodes:
            result = node.put_fileX(cvsfile)
            key    = result[0]['keyHref']
            r      = node.get_key(key)
            f      = open(cvsfile)
            self.diff(r, f)
            f.close()

    def diff(self,r, f):
        for (r_chunk,f_chunk) in itertools.izip(r.iter_content(1024), chunked_file(f, 1024)):
            self.assertEqual(r_chunk,f_chunk)



#    def test_B_large_putfile(self):
#        cvsfile = h2o.find_file('/tmp/Rango.avi')
#        node = h2o.nodes[0]
#        node.put_fileX(cvsfile)


if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
