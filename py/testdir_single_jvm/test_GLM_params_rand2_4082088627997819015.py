import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

# none is illegal for threshold
# always run with xval, to make sure we get the trainingErrorDetails
# family gaussian gives us there
# Exception in thread "Thread-6" java.lang.RuntimeException: Matrix is not symmetric positive definite.
# at Jama.CholeskyDecomposition.solve(CholeskyDecomposition.java:173)


# Hardwire to norm=L2 to avoid gram matrix problem?
# try always using L1 or L2
def define_params():
    if h2o.new_json:
        paramDict = {
            'Y': [54],
            'X': [0,1,15,33,34],
            'glm_-X': [None,'40:53'],
            'family': ['binomial'],
            'xval': [2,3,4,9,15],
            'threshold': [0.1, 0.5, 0.7, 0.9],
            'norm': ['L1', 'L2'],

            'lambda1': [None, 1e-8, 1e-4,1,10,1e4],
            'lambda2': [None, 1e-8, 1e-4,1,10,1e4],
            'rho': [None, 1e-4,1,10,1e4],
            # alpha must be between -1 and 1.8?
            'alpha': [None, -1,0,1,1.8],
            'beta_eps': [None, 0.0001],
            'case': [1,2,3,4,5,6,7],
            # inverse and log causing problems
            # 'link': [None, 'logit','identity', 'log', 'inverse'],
            'max_iter': [None, 10],
            'weight': [None, 1, 2, 4],
            }

    else:
        paramDict = {
            'Y': [54],
            'X': [0,1,15,33,34],
            'glm_-X': [None,'40:53'],
            'family': ['binomial'],
            'xval': [2,3,4,9,15],
            'threshold': [0.1, 0.5, 0.7, 0.9],
            'norm': ['L1', 'L2'],
            'glm_lambda': [None, 1e-4,1,10,1e4],
            'rho': [None, 1e-4,1,10,1e4],
            'alpha': [None, 1e-4,1,10,1e4],
            }
    return paramDict

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        paramDict = define_params()
        for trial in range(40):
            # default
            colX = 0 
            # form random selections of RF parameters
            # always need Y=54. and always need some xval (which can be overwritten)
            # with a different choice. we need the xval to get the error details 
            # in the json(below)
            # force family=binomial to avoid the assertion error above with gaussian
            # seed norm to L2, so have that or the selection above
            kwargs = {'Y': 54, 'xval' : 3, 'family' : 'binomial', 'norm' : 'L2', 'max_iter' : 5, 'case': 1}
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
            glm = h2o_cmd.runGLMOnly(timeoutSecs=150, parseKey=parseKey, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            # FIX! I suppose we have the problem of stdout/stderr not having flushed?
            # should hook in some way of flushing the remote node stdout/stderr
            if (h2o.check_sandbox_for_errors()):
                raise Exception("Found errors in sandbox stdout or stderr, on trial #%s." % trial)

            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
