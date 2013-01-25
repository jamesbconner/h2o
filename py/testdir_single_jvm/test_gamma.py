import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd
import h2o_glm

def define_params():
    paramDict = {
        # 'x': ['0:3','14:17','5:10',0,1,15,33],
        'x': [0,1,15,33],
        # 'family': [None, 'gaussian', 'binomial', 'poisson', 'gamma'],
        'family': ['gamma'],
        # 'num_cross_validation_folds': [2,3,4,9],
        'num_cross_validation_folds': [0],
        'thresholds': [0.1, 0.5, 0.7, 0.9],
        'lambda': [1e-4],
        # 0 is L2. 1 is L1. Inbetween is Elastic
        'alpha': [0,0.5,0.75],
        # coefficient precision
        'beta_epsilon': [None, 0.0001],
        'case': [1,2,3,4,5,6,7],
        'case_mode': ['>','<','=','<=','>='],
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        # 'link': [None, 'logit','identity'],
        # This is the new name? fine, we don't care for old or old testing (maxIter?)
        'max_iter': [3,7,11],
        'weight': [None, 1, 2, 4],
        }
    return paramDict

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_gamma(self):
        # csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
        print "csvPathname:", csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        # SEED = random.randint(0, sys.maxint)
        SEED = 8977501266014959103
        # if you have to force to redo a test
        # SEED =
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        paramDict = define_params()
        for trial in range(20):
            # default
            colX = 0
            # form random selections of GLM parameters
            # always need Y=54. and always need some num_cross_validation_folds (which can be overwritten)
            # with a different choice. we need the num_cross_validation_folds to get the error details
            # in the json(below)
            kwargs = {'y': 54, 'alpha': 0, 'lambda': 0, 'case_mode': '=', 'case': 1, 'max_iter': 6}
            randomGroupSize = random.randint(1,len(paramDict))
            for i in range(randomGroupSize):
                randomKey = random.choice(paramDict.keys())
                randomV = paramDict[randomKey]
                randomValue = random.choice(randomV)
                kwargs[randomKey] = randomValue

                if (randomKey=='x'):
                    colX = randomValue

            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=70, parseKey=parseKey, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
