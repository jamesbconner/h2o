import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

# none is illegal for threshold
# always run with xval, to make sure we get the trainingErrorDetails
# family gaussian gives us there
# Exception in thread "Thread-6" java.lang.RuntimeException: Matrix is not symmetric positive definite.
# at Jama.CholeskyDecomposition.solve(CholeskyDecomposition.java:173)

# FIX! we'll have to do something for gaussian. It doesn't return the ted keys below
paramDict = {
    'Y': [54],
    'X': [0,1,15,33,34],
    '-X': [None,'40:53'],
    'family': ['binomial', 'poisson'],
    'xval': [2,3,4,9,15],
    'threshold': [0.1, 0.5, 0.7, 0.9],
    'norm': [None,'L1', 'L2'],
    'glm_lamba': [None, 1e-4,1,10,1e4],
    'rho': [None, 1e-4,1,10,1e4],
    'alpha': [None, 1e-4,1,10,1e4],
    }

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_covtype(self):

        def simpleCheckGLM(glm,colX):
            # h2o GLM will verboseprint the result and print errors. 
            # so don't have to do that
            # different when xvalidation is used? No trainingErrorDetails?
            print "GLM time", glm['time']
            tsv = glm['trainingSetValidation']
            print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']

            glmParams = glm["glmParams"]
            family = glmParams["family"]
            # no trainingErrorDetails if poisson? 
            if (family=="poisson"):
                pass
            else:
                ted = glm['trainingErrorDetails']
                print "trueNegative:", ted['trueNegative']
                print "truePositive:", ted['truePositive']
                print "falseNegative:", ted['falseNegative']
                print "falsePositive:", ted['falsePositive']

            # it's a dicitionary!
            coefficients = glm['coefficients']
            print "\ncoefficients:", coefficients
            # pick out the coefficent for the column we enabled.
            absXCoeff = abs(float(coefficients[str(colX)]))
            # intercept is buried in there too
            absIntercept = abs(float(coefficients['Intercept']))

            if (1==0):
                self.assertGreater(absXCoeff, 0.000001, (
                    "abs. value of GLM coefficients['" + str(colX) + "'] is " +
                    str(absXCoeff) + ", not >= 0.000001 for X=" + str(colX)
                    ))

                self.assertGreater(absIntercept, 0.000001, (
                    "abs. value of GLM coefficients['Intercept'] is " +
                    str(absIntercept) + ", not >= 0.000001 for X=" + str(colX)
                    ))


        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        for trial in range(20):
            # default
            colX = 0 
            # form random selections of RF parameters
            # always need Y=54. and always need some xval (which can be overwritten)
            # with a different choice. we need the xval to get the error details 
            # in the json(below)
            # force family=binomial to avoid the assertion error above with gaussian
            kwargs = {'Y': 54, 'xval' : 3, 'family' : "binomial"}
            randomGroupSize = random.randint(1,len(paramDict))
            for i in range(randomGroupSize):
                randomKey = random.choice(paramDict.keys())
                randomV = paramDict[randomKey]
                randomValue = random.choice(randomV)
                kwargs[randomKey] = randomValue

                if (randomKey=='X'):
                    # keep track of what column we're picking
                    colX = randomValue

            print kwargs
            
            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=70, parseKey=parseKey, **kwargs)
            simpleCheckGLM(glm,colX)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
