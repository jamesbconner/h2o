import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o
import time

class Basic(unittest.TestCase):
    def test_Cloud(self):
        base_port = 54300
        ports_per_node = 3
        for trials in range(0,2):
            for tryNodes in range(3,5):
                sys.stdout.write('.')
                sys.stdout.flush()

                start = time.time()
                # start by cleaning sandbox (in build_cloud). 
                # so nosetest works which doesn't do unit_main
                h2o.build_cloud(node_count=tryNodes,timeoutSecs=30)
                print "loop %d: Build cloud of %d in %d s" % (trials, tryNodes, (time.time() - start)) 

                c = h2o.nodes[0].get_cloud()
                self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')
                h2o.tear_down_cloud()
                # with so many jvms, wait for sticky ports to be freed up..slow os stuff?
                # changed, to increment the base_port, to avoid reuse immediately
                time.sleep(4)
                base_port += ports_per_node * tryNodes


if __name__ == '__main__':
    h2o.unit_main()
