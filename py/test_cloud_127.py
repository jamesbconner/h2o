import os, json, unittest, time, shutil, sys 
import h2o

class Basic(unittest.TestCase):

    def testCloud(self):
        global nodes

        baseport = 54300
        for tryNodes in range(2,10):
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            nodes = h2o.build_cloud(use_this_ip_addr="127.0.0.1", node_count=tryNodes,timeoutSecs=30)
            print "Build cloud of %d in %d secs" % (tryNodes, (time.time() - start)) 

            h2o.verboseprint(nodes)
            for n in nodes:
                c = n.get_cloud()
                h2o.verboseprint(c)
                self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

            h2o.tear_down_cloud(nodes)

            # can't talk to cloud after we tear it down. This will fail
            # FIX! I suppose tear down should be checked somehow at some point

            # increment the base_port to avoid sticky ports when we do another
            baseport += 3 * tryNodes

if __name__ == '__main__':
    h2o.unit_main()
