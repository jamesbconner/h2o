import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uses two much memory with 4?
        h2o.build_cloud(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_B_covtype_single_cols(self):

        def simpleCheckGLM(glm,colX):
            # h2o GLM will verboseprint the result and print errors. 
            # so don't have to do that
            # different when xvalidation is used? No trainingErrorDetails?
            print "GLM time", glm['time']
            tsv = glm['trainingSetValidation']
            print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']

            ted = glm['trainingErrorDetails']
            print "trueNegative:", ted['trueNegative']
            print "truePositive:", ted['truePositive']
            print "falseNegative:", ted['falseNegative']
            print "falsePositive:", ted['falsePositive']

            # it's a dicitionary!
            coefficients = glm['coefficients']
            print "\ncoefficients:", coefficients
            # pick out the coefficent for the column we enabled. This only works if no labels were in the dataset
            # because we're using col for the key
            absXCoeff = abs(float(coefficients[str(colX)]))
            # intercept is buried in there too
            absIntercept = abs(float(coefficients['Intercept']))
            
            self.assertGreater(absXCoeff, 0.000001, (
                "abs. value of GLM coefficients['" + str(colX) + "'] is " + 
                str(absXCoeff) + ", not >= 0.000001 for X=" + str(colX)
                ))

            self.assertGreater(absIntercept, 0.000001, (
                "abs. value of GLM coefficients['intercept'] is " + 
                str(absIntercept) + ", not >= 0.000001 for X=" + str(colX)
                ))

        timeoutSecs = 2
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname

        # columns start at 0
        Y = "54"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        print "GLM binomial wth 1 X column at a time" 
        print "Result check: abs. value of coefficient and intercept returned are bigger than zero"
        for colX in xrange(55):
            # do we have to exclud any columns?
            if (1==1):
                if X == "": 
                    X = str(colX)
                else:
                    # X = X + "," + str(colX)
                    X = str(colX)

                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, xval=6, X=X, Y=Y, timeoutSecs=timeoutSecs)

                simpleCheckGLM(glm,colX)
                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
