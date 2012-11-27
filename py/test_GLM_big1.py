import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_C_hhp_107_01(self):
        timeoutSecs = 2
        
        csvPathname = "../smalldata/hhp_107_01.data.gz"
        print "\n" + csvPathname

        Y = "106"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        # create the X that excludes some columns
        for appendX in xrange(107):
            if (appendX == 9):
                print "9 causes singularity. not used"
            elif (appendX == 12): 
                print "12 causes singularity. not used"
            elif (appendX == 25): 
                print "25 causes singularity. not used"
            elif (appendX == 53): 
                print "53 causes singularity. not used"
            elif (appendX == 54): 
                print "54 causes singularity. not used"
            elif (appendX == 76): 
                print "76 causes singularity. not used"
            elif (appendX == 91): 
                print "91 causes singularity. not used"
            elif (appendX == 103): 
                print "103 causes singularity. not used"
            elif (appendX == 106):
                print "106 is output."
            else:
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

        # run it times
        for trial in xrange(3):
                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                ### FIX! add some expected result checking
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, xval=6, X=X, Y=Y, timeoutSecs=timeoutSecs)

                h2o.verboseprint("\nglm:", glm)
                print "\nTrial #", trial
                print "errRate:", glm['errRate']
                print "trueNegative:", glm['trueNegative']
                print "truePositive:", glm['truePositive']
                print "falseNegative:", glm['falseNegative']
                print "falsePositive:", glm['falsePositive']
                print "coefficients:", glm['coefficients']
                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
