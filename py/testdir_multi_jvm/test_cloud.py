import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o

class Basic(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        # this is for safety after error, plus gets us the grep of stdout/stderr for errors
        h2o.tear_down_cloud()

    def test_Cloud(self):
        # FIX! weird timeout H2O exceptions with >8? maybe shouldn't
        # don't know if we care
        for tryNodes in range(2,8):
            h2o.verboseprint("Trying cloud of", tryNodes)
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            h2o.build_cloud(node_count=tryNodes)
            print "Built cloud of %d in %d s" % (tryNodes, (time.time() - start)) 

            c = h2o.nodes[0].get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')
            h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.unit_main()
