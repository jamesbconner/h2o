import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd as cmd


class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        nodes = h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_A_Basic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

   
    def test_C_prostate(self):
        timeoutSecs = 2
        print "\nStarting prostate.csv"
        # columns start at 0
        Y = "1"
        X = ""
        csvFilename = "prostate.csv"
        csvPathname = "../smalldata/logreg" + '/' + csvFilename
        put = nodes[0].put_file(csvPathname)
        parseKey = nodes[0].parse(put['key'])
        parseKey['key'] = parseKey['Key']
        glm = nodes[0].glm(parseKey=parseKey, args={'Y':'CAPSULE','family':'binomial'}, timeoutSecs=timeoutSecs)
        h2o.verboseprint("\nglm:", glm)


if __name__ == '__main__':
    h2o.unit_main()
