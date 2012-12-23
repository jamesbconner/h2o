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

    def test_B_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname

        # columns start at 0
        Y = "54"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname,timeoutSecs=10)

        for appendX in xrange(55):
            if X == "": 
                X = str(appendX)
            else:
                X = X + "," + str(appendX)

            # only run if appendX is > 49 to save time
            # we need to cycle up to there though, to get the parameters right for GLM json
            if (appendX>49):
                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                # norm=L2 to avoid coefficients = 0 in result?
                kwargs = {'X': X, 'Y': Y, 'norm': 'L2', 'xval': 3}
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
                h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
