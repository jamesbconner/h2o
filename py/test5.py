import os, json, unittest, time, shutil, sys
import h2o

def putValue(n,value,key=None,repl=None):
    put = n.put_value(value,key,repl)
    return put['keyHref']

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global verbose
        verbose = True

        h2o.clean_sandbox()
        global nodes
        print "1 node"
        nodes = h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_Basic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

    def test1(self):
        timeoutSecs = 10
        trial = 1

        for x in xrange (1,2000,1):
            if ((x % 100) == 0):
                sys.stdout.write('.')
                sys.stdout.flush()

            trialString = "Trial" + str(trial)
            trialStringXYZ = "Trial" + str(trial) + "XYZ"
            putKey = putValue(nodes[0],trialString,key=trialStringXYZ,repl=None)
            ### print "putKey:", putKey

            ### print trialString
            trial += 1

if __name__ == '__main__':
    unittest.main()
