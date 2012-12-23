import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uses two much memory with 4?
        # FIX! does it die with 3?
        h2o.build_cloud(2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

        timeoutSecs = 10
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname

        # columns start at 0
        Y = "54"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        print "GLM binomial wth 1 X column at a time" 
        print "Result check: abs. value of coefficient and intercept returned are bigger than zero"
        for colX in xrange(54):
            # do we have to exclud any columns?
            if (1==1):
                if X == "": 
                    X = str(colX)
                else:
                    # X = X + "," + str(colX)
                    X = str(colX)

                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                kwargs = {'X': X, 'Y': Y, 'xval': 6}
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
                h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)
                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
