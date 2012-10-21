import os, json, unittest, time, shutil, sys, time
import h2o, h2o_cmd, h2o_hosts
import itertools

def file_to_put():
    # kbn fails 10/15/12
    return 'hdfs://192.168.1.151/datasets/covtype.data'

# -rw-r--r--   3 hduser supergroup    41169365 2012-09-14 12:48 /datasets/hhp_9_14_12.data
# -rw-r--r--   3 hduser supergroup    48381802 2012-09-04 13:08 /datasets/hhp2.os.noisy.0_1.data
# -rw-r--r--   3 hduser supergroup    48397103 2012-09-04 13:08 /datasets/hhp2.os.noisy.9_4.data
# -rw-r--r--   3 hduser supergroup    75169317 2012-08-27 02:59 /datasets/covtype.data

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts(use_hdfs=True)

    @classmethod
    def tearDownClass(cls):
        h2o.verboseprint("Tearing down cloud")
        h2o.tear_down_cloud()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # Try to put a file to each node in the cloud and checked reported size of the saved file 
    def test_A_putfile_to_all_nodes(self):
        
        cvsfile  = h2o.find_file(file_to_put())
        origSize = h2o.get_file_size(cvsfile)

        # Putfile to each node and check the returned size
        for node in h2o.nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file:", cvsfile, "node:", node, "origSize:", origSize)
            result     = node.put_file(cvsfile)
            returnSize = result['size']
            self.assertEqual(origSize,returnSize)

    # Try to put a file, get file and diff orinal file and returned file.
    def test_B_putfile_and_getfile_to_all_nodes(self):

        # FIX! if it's hdfs: we won't find it?
        # cvsfile = h2o.find_file(file_to_put())
        cvsfile = file_to_put()

        nodeTry = 0
        for node in h2o.nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file", cvsfile, "to", node)
            result = node.put_file(cvsfile)
            h2o.verboseprint("put_file ok for node", nodeTry)

            key    = result['keyHref']
            r      = node.get_key(key)
            f      = open(cvsfile)
            self.diff(r, f)
            h2o.verboseprint("put_file filesize ok")
            f.close()
            nodeTry += 1

    def diff(self,r, f):
        h2o.verboseprint("checking r and f:", r, f)
        for (r_chunk,f_chunk) in itertools.izip(r.iter_content(1024), h2o.iter_chunked_file(f, 1024)):
            self.assertEqual(r_chunk,f_chunk)


if __name__ == '__main__':
    h2o.unit_main()
    print "hello2"

