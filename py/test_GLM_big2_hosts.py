import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import time


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

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
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        # build up the parameter string in X
        Y = "106"
        X = ""
        # for appendX in range(1,107):
        for appendX in range(1,107):
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

        # go right to the big X and iterate on that case
        for trial in range(2):
            print "\nTrial #", trial, "start"
            print "\nX:", X
            print "Y:", Y

            start = time.time()
            ### FIX! add some expected result checking
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, xval=7, X=X, Y=Y, timeoutSecs=timeoutSecs)

            print "\nTrial #", trial
            h2o.verboseprint("\nglm:", glm)
            print "errRate:", glm['errRate']
            print "trueNegative:", glm['trueNegative']
            print "truePositive:", glm['truePositive']
            print "falseNegative:", glm['falseNegative']
            print "falsePositive:", glm['falsePositive']
            print "coefficients:", glm['coefficients']
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            # maybe we can see the GLM results in a browser?
            # FIX! does it recompute it??
            h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            # wait in case it recomputes it
            time.sleep(10)


            sys.stdout.write('.')
            sys.stdout.flush() 

        # browseJsonHistoryAsUrl()

if __name__ == '__main__':
    h2o.unit_main()
