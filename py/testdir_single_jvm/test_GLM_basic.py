import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd


class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        nodes = h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

    def test_B_benign(self):
        print "\nStarting benign.csv"
        timeoutSecs = 2
        
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

                ### FIX! add some expected result checking
                glm = h2o_cmd.runGLM(csvPathname=csvPathname, X=X, Y=Y, xval=4,
                    timeoutSecs=timeoutSecs)

                h2o.verboseprint("glm: ", glm)
                print "\ncoefficients:", glm['coefficients']

    def test_C_prostate(self):
        timeoutSecs = 2
        

        print "\nStarting prostate.csv"
        # columns start at 0
        Y = "1"
        X = ""
        csvFilename = "prostate.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

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

                ### FIX! add some expected result checking
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, X=X, Y=Y, xval=5, timeoutSecs=timeoutSecs)

                # different json entries when xvalidation is used? No trainingErrorDetails?
                # h2o GLM does dump of json result now with verbose

                if 'warnings' in glm:
                    print "\nwarnings:", glm['warnings']

                print "GLM time", glm['time']
                coefficients = glm['coefficients']
                print "coefficients:", coefficients
                # quick and dirty check: if all the coefficients are zero, something is broken
                # intercept is in there too, but this will get it okay
                # just sum the abs value  up..look for greater than 0
                s = 0.0
                for c in coefficients:
                    v = coefficients[c]
                    s += abs(float(v))
                    self.assertGreater(s, 0.000001, (
                        "sum of abs. value of GLM coefficients/intercept is " + str(s) + ", not >= 0.000001"
                        ))

                tsv = glm['trainingSetValidation']
                print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']
                ted = glm['trainingErrorDetails']
                print "trueNegative:", ted['trueNegative']
                print "truePositive:", ted['truePositive']
                print "falseNegative:", ted['falseNegative']
                print "falsePositive:", ted['falsePositive']


if __name__ == '__main__':
    h2o.unit_main()
