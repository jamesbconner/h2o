import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        nodes = h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_benign(self):
        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename)
        # columns start at 0
        Y = "3"
        X = ""
        # cols 0-13. 3 is output
        # no member id in this one
        for appendX in xrange(14):
            if (appendX == 1): 
                print "\nSkipping 1. Causes NaN. Ok now, later though?"
            elif (appendX == 2): 
                print "\nSkipping 2. Causes NaN. Ok now, later though?"
            elif (appendX == 3): 
                print "\n3 is output."
            else:
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

                sys.stdout.write('.')
                sys.stdout.flush()
                csvFilename = "benign.csv"
                csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
                print "\nX:", X
                print "Y:", Y

                kwargs = {'X': X, 'Y':  Y, 'xval': 4}
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
                h2o_glm.simpleCheckGLM(self, glm, 'STR', **kwargs)

    def test_C_prostate(self):
        print "\nStarting prostate.csv"
        # columns start at 0
        Y = "1"
        X = ""
        csvFilename = "prostate.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename)

        for appendX in xrange(9):
            if (appendX == 0):
                print "\n0 is member ID. not used"
            elif (appendX == 1):
                print "\n1 is output."
            elif (appendX == 7): 
                print "\nSkipping 7. Causes NaN. Ok now, later though?"
            else:
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                kwargs = {'X': X, 'Y':  Y, 'xval': 5}
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=2, **kwargs)
                # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
                h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
