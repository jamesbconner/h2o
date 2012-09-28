import os, json, unittest, time, shutil, sys
import h2o

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
        for tryNodes in range(2,11):
            start = time.time()
            h2o.build_cloud(node_count=tryNodes)
            print "Build cloud of %d in %d s" % (tryNodes, (time.time() - start)) 

            # put variable delay here or no?
            # can check a couple of things about cloud if we want..
            # example: {u'cloud_name': u'kevin', u'cloud_size': 1, u'node_name': u'/10.0.2.15:54322'}
            time.sleep(1.0)
            c = h2o.nodes[0].get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')
            h2o.tear_down_cloud()

if __name__ == '__main__':
    unittest.main()
