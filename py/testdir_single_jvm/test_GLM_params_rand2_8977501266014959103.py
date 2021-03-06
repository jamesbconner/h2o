import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd
import h2o_glm

def define_params():
    paramDict = {
        'family': [None, 'gaussian', 'binomial', 'poisson'],
        'num_cross_validation_folds': [2,3,4,9],
        'thresholds': [0.1, 0.5, 0.7, 0.9],
        'lambda': [0, 1e-4],
        'alpha': [0,0.5,0.75],
        # new?
        'beta_epsilon': [None, 0.0001],
        # too many problems with case=7
        'case': [1,2,3,4,5,6],
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        # 'link': [None, 'logit','identity'],
        # This is the new name? fine, we don't care for old or old testing (maxIter?)
        'max_iter': [None, 10],
        'weight': [None, 1, 2, 4],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_params_rand2_8977501266014959103(self):
        # csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
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
            # params is mutable. This is default.
            params = {'y': 54, 'alpha': 0, 'lambda': 0, 'case': 1}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=70, parseKey=parseKey, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
