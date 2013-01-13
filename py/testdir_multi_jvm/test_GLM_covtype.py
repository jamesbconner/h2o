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
        start = time.time()
        kwargs = {'y': Y, 'norm': 'L2', 'xval': 3, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
