import os, json, unittest, time, shutil, sys
import h2o, cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
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

    def test_D_GenParity1(self):
        timeoutSecs = 2
        
        # columns start at 0
        Y = "1"
        X = ""
        for appendX in (2,3,4,5,6,8):
            sys.stdout.write('.')
            sys.stdout.flush()
            if (appendX == 2): 
                X = "2"
            else:
                X = X + "," + str(appendX)

            csvFilename = "prostate.csv"
            csvPathname = "../smalldata/logreg" + '/' + csvFilename
            print "X:", X
            print "Y:", Y

            ### FIX! add some expected result checking
            ### ..{u'h2o': u'/192.168.1.17:54321', u'Intercept': -0.25720656777427364, u'ID': -0.000723962423344251, u'response_html': u'<div class=\'alert alert-success\'>Linear regression on data <a href=____6aebc-a37f-465e-bd4e-3bd0a3a5828c>6aebc-a37f-465e-bd4e-3bd0a3a5828c</a> computed in 21[ms]<strong>.</div><div class="container">Result Coeficients:<div>ID = -7.23962423344251E-4</div><div>Intercept = -0.25720656777427364</div></div>', u'time': 21}
            glm = cmd.runGLM(csvPathname=csvPathname, X=X, Y=Y, timeoutSecs=timeoutSecs)
            # print "glm:", glm
            print "\nAGE:", glm['AGE']
            print "Intercept:", glm['Intercept']


if __name__ == '__main__':
    unittest.main()
