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

        global default_kwargs
        default_kwargs = {
            'y': 54, 
            'case': 1, 
            'case_mode': '=', 
            'alpha': 0.5, 
            'lambda': 1e1,
            'max_iter': 15,
            'x_value': 2,
            }

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_covtype_case_1(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 1
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_B_covtype_case_2(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 2
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    # FIX! fails
    def test_C_covtype_case_3(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 3
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_D_covtype_case_4(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 4
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_E_covtype_case_5(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 5
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_F_covtype_case_6(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 6
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

    def test_G_covtype_case_7(self):
        start = time.time()
        kwargs = default_kwargs
        kwargs['case'] = 7
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=180, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
