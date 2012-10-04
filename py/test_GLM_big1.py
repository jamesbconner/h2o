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

    def test_C_hhp_107_01(self):
        timeoutSecs = 2
        
        print "\nStarting hhp_107_01.data"
        # columns start at 0
        # FIX! seemed to hang if I said 107 (non-existent)
        # How are these supposed to be dealt with?
        # Exception in thread "Thread-7" water.web.GLM$InvalidInputException: invalid Y value, column 107 does not exist!

        Y = "106"
        X = ""
        # FIX! just a small number for no
        # for appendX in xrange(107):
        for appendX in xrange(107):
            if (appendX == 9):
                print "\n9 causes singularity. not used"
            elif (appendX == 12): 
                print "\n12 causes singularity. not used"
            elif (appendX == 25): 
                print "\n25 causes singularity. not used"
            elif (appendX == 53): 
                print "\n53 causes singularity. not used"
            elif (appendX == 54): 
                print "\n54 causes singularity. not used"
            elif (appendX == 76): 
                print "\n76 causes singularity. not used"
            elif (appendX == 91): 
                print "\n91 causes singularity. not used"
            elif (appendX == 103): 
                print "\n103 causes singularity. not used"
            elif (appendX == 106):
                print "\n106 is output."
            else:
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

                sys.stdout.write('.')
                sys.stdout.flush() 
                csvFilename = "hhp_107_01.data"
                csvPathname = "../smalldata" + '/' + csvFilename
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                ### FIX! add some expected result checking
                glm = cmd.runGLM(csvPathname=csvPathname, X=X, Y=Y, timeoutSecs=timeoutSecs)
                print "glm:", glm
                # print "AGE:", glm['AGE']
                print "Intercept:", glm['Intercept']
                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'



if __name__ == '__main__':
    h2o.unit_main()
