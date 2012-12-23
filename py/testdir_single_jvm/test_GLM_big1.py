import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_C_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname

        Y = "106"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=2)

        # create the X that excludes some columns
        trial = 0
        for appendX in xrange(107):
            if (appendX == 9):
                print "9 causes singularity. not used"
            elif (appendX == 12): 
                print "12 causes singularity. not used"
            elif (appendX == 25): 
                print "25 causes singularity. not used"
            elif (appendX == 53): 
                print "53 causes singularity. not used"
            elif (appendX == 54): 
                print "54 causes singularity. not used"
            elif (appendX == 76): 
                print "76 causes singularity. not used"
            elif (appendX == 91): 
                print "91 causes singularity. not used"
            elif (appendX == 103): 
                print "103 causes singularity. not used"
            elif (appendX == 106):
                print "106 is output."
            else:
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

        for trial in xrange(3):
            sys.stdout.write('.')
            sys.stdout.flush() 
            print "\nX:", X
            print "Y:", Y

            start = time.time()
            kwargs = {'X': X, 'Y': Y, 'xval': 6}
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=300, **kwargs)

            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
