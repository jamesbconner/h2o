import os, json, unittest, time, shutil, sys 
import h2o

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # NOTE: unittest will run tests in an arbitrary order..not constrained to order here.
    # Possible hack: change the names so this test order matches alphabetical order
    # by using intermediate "_A_" etc. 
    # That should make unittest order match order here? 

    def test_Cloud(self):
        global nodes

        for tryNodes in range(2,10):
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            nodes = h2o.build_cloud(addr="127.0.0.1",node_count=tryNodes)
            print "Build cloud of %d in %d secs" % (tryNodes, (time.time() - start)) 

            h2o.verboseprint(nodes)
            for n in nodes:
                c = n.get_cloud()
                h2o.verboseprint(c)
                self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

            h2o.tear_down_cloud(nodes)

            # can't talk to cloud after we tear it down. This will fail
            # FIX! I suppose tear down should be checked somehow at some point

if __name__ == '__main__':
    h2o.unit_main()
