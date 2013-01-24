import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uses two much memory with 4?
        h2o.build_cloud(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname

        # columns start at 0
        y = "54"
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname,timeoutSecs=10)

        max_iter = 30

        # L2 
        kwargs = {'y': y, 'num_cross_validation_folds': 0, 'case': 1, 'alpha': 0, 'lambda': 0, 'max_iter': max_iter}
        start = time.time()
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # Elastic
        kwargs = {'y': y, 'num_cross_validation_folds': 0, 'case': 1, 'alpha': 0.5, 'lambda': 1e-4, 'max_iter': max_iter}
        start = time.time()
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # L1
        kwargs = {'y': y, 'num_cross_validation_folds': 0, 'case': 1, 'alpha': 1.0, 'lambda': 1e-4, 'max_iter': max_iter}
        start = time.time()
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
