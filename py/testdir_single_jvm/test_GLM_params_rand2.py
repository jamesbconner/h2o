import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd, h2o_glm

# none is illegal for threshold
# always run with xval, to make sure we get the trainingErrorDetails
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

        # csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
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

            # everything is under GLMModel now?
            h2o_glm.simpleCheckGLM(glm,colX)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
