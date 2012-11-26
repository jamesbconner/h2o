import os, json, unittest, time, shutil, sys
import h2o
import time

class Basic(unittest.TestCase):
    def test_Cloud(self):
        for trials in range(0,2):
            for tryNodes in [12]:
                sys.stdout.write('.')
                sys.stdout.flush()

                start = time.time()
                h2o.build_cloud(node_count=tryNodes,timeoutSecs=30)
                print "loop %d: Build cloud of %d in %d s" % (trials, tryNodes, (time.time() - start)) 

                c = h2o.nodes[0].get_cloud()
                self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')
                h2o.tear_down_cloud()
                h2o.clean_sandbox()
                # with so many jvms, wait for sticky ports to be freed up..slow os stuff?
                time.sleep(4)

if __name__ == '__main__':
    h2o.unit_main()
