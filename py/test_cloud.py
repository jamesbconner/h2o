import os, json, unittest, time, shutil, sys
import util.h2o as h2o



def getCloud():
    for n in nodes:
        c = n.get_cloud()
        h2o.verboseprint("get_cloud:", c)
        return(c)

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()

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

        for tryNodes in range(2,11):
            start = time.time()
            nodes = h2o.build_cloud(node_count=tryNodes)
            print "Build cloud of %d in %d s" % (tryNodes, (time.time() - start)) 

            # put variable delay here or no?
            # can check a couple of things about cloud if we want..
            # example: {u'cloud_name': u'kevin', u'cloud_size': 1, u'node_name': u'/10.0.2.15:54322'}
            time.sleep(1.0)
            c = getCloud()
            h2o.verboseprint("test_Cloud:", c)
            h2o.verboseprint ("test_Cloud nodes:", nodes)
            self.assertEqual(c['cloud_size'], len(nodes), 'Check1: inconsistent cloud size')

            h2o.tear_down_cloud(nodes)

            # can't talk to cloud after we tear it down. This will fail
            # FIX! I suppose tear down should be checked somehow at some point
            # c = getCloud()

if __name__ == '__main__':
    unittest.main()
