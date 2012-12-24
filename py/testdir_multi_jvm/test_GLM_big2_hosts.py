import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_hosts, h2o_glm
import h2o_browse as h2b
import time

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            # maybe fails more reliably with just 2 jvms?
            h2o.build_cloud(2,java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_C_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        # build up the parameter string in X
        Y = "106"
        X = ""
        for appendX in range(1,107):
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

        # go right to the big X and iterate on that case
        for trial in range(2):
            print "\nTrial #", trial, "start"
            print "\nX:", X
            print "Y:", Y

            start = time.time()
            kwargs = {'X': X, 'Y': Y, 'xval': 7}
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)

            ### h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
