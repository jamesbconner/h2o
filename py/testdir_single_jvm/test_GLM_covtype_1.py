import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uses two much memory with 4?
        h2o.build_cloud(1)

        global csvPathname, parseKey, y
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname,timeoutSecs=10)
        # columns start at 0
        y = '54'

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    # FIX! fails
    def test_A_covtype_no_xval(self):
        start = time.time()
        kwargs = {'y': y, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm no xval end on ", csvPathname, 'took', time.time() - start, 'seconds'

    # FIX! fails
    def test_B_covtype_xval_0(self):
        start = time.time()
        kwargs = {'y': y, 'xval': 0, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm xval=0 end on ", csvPathname, 'took', time.time() - start, 'seconds'

    # FIX! fails
    def test_C_covtype_xval_1(self):
        start = time.time()
        kwargs = {'y': y, 'xval': 1, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm xval=1 end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_D_covtype_xval_2(self):
        start = time.time()
        kwargs = {'y': y, 'xval': 2, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm xval=2 end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_E_covtype_xval_10(self):
        start = time.time()
        kwargs = {'y': y, 'xval': 10, 'case': 1}
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm xval=10 end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
