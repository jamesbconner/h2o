import os, json, unittest, time, shutil, sys
import h2o

class Basic(unittest.TestCase):
    def test_Cloud(self):
        for tryNodes in range(2,11):
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            h2o.build_cloud(node_count=tryNodes)
            print "Build cloud of %d in %d s" % (tryNodes, (time.time() - start)) 

            c = h2o.nodes[0].get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')
            h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
