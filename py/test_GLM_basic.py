import os, json, unittest, time, shutil, sys
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
                csvPathname = "../smalldata/logreg" + '/' + csvFilename
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
        csvPathname = "../smalldata/logreg" + '/' + csvFilename
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

                # {
                # "key":"1_100kx7_logreg.data.hex",
                # "h2o":"/192.168.0.37:55320",
                # "name":"Logistic regression",
                # "glmParams":{"link":"logit","family":"binomial"},
                # "lsmParams":{"norm":"NONE","lambda":0.0,"rho":0.01,"alpha":1.0},
                # "warnings":["Failed to converge due to NaNs"],
                # "rows":100000,
                # "time":148,
                # "coefficients":{"0":9.200235113946587,"1":0.5845263465847916,"2":4.243632149713497,"3":0.8381294453405328,"4":0.8222999328856485,"5":32.25703239247885,"6":0.07752460411110679, "Intercept":-124.05198738651708},
                # "trainingSetValidation":{"DegreesOfFreedom":99999,"ResidualDegreesOfFreedom":99992,"NullDeviance":"19538990.016","ResidualDeviance":"2907.0454","AIC":"2923.0454","trainingSetErrorRate":"0.0003"},
                # "trainingErrorDetails":{"falsePositive":"0.0001","falseNegative":"0.0002","truePositive":"0.1377","trueNegative":"0.862"}
                # }

                # 'xfactor': 5, 
                # 'threshold': 0.5, 

                # different when xvalidation is used? No trainingErrorDetails?
                h2o.verboseprint("\nglm:", glm)
                if 'warnings' in glm:
                    print "\nwarnings:", glm['warnings']

                print "GLM time", glm['time']
                print "coefficients:", glm['coefficients']
                print glm

                tsv = glm['trainingSetValidation']
                print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']
                # ted = glm['trainingErrorDetails']

                # print "trueNegative:", ted['trueNegative']
                # print "truePositive:", ted['truePositive']
                # print "falseNegative:", ted['falseNegative']
                # print "falsePositive:", ted['falsePositive']


if __name__ == '__main__':
    h2o.unit_main()
