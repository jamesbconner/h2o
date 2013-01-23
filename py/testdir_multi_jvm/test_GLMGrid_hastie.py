
## Dataset created from this:
#
# from sklearn.datasets import make_hastie_10_2
# import numpy as np
# i = 1000000
# f = 10
# (X,y) = make_hastie_10_2(n_samples=i,random_state=None)
# y.shape = (i,1)
# Y = np.hstack((X,y))
# np.savetxt('./1mx' + str(f) + '_hastie_10_2.data', Y, delimiter=',', fmt='%.2f');

import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_hosts
import copy

def glm_doit(self, csvFilename, csvPathname, timeoutSecs=30):
    print "\nStarting parse of", csvFilename
    parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename, timeoutSecs=10)
    y = "10"
    x = ""
    # NOTE: hastie has two values, -1 and 1. To make H2O work if two valued and not 0,1 have
    kwargs = {
        'x': x, 'y':  y, 'case': '1', 'destination_key': 'gg',
        'x_value': 0,
        'penalty': '1e-8:1e3:100',
        'alpha': '0,0.25,0.8,1.0',
        'thresholds': '0:1:0.01'
        }

    start = time.time() 
    print "\nStarting GLMGrid of", csvFilename
    glmGridResult = h2o_cmd.runGLMGridOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
    print "GLMGrid in",  (time.time() - start), "secs (python)"

    h2o_glm.simpleCheckGLMGrid(self,glmGridResult, **kwargs)

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            h2o.build_cloud(2,java_heap_GB=6)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_1mx10_hastie_10_2(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = h2o.find_dataset('logreg' + '/' + csvFilename)
        glm_doit(self,csvFilename, csvPathname, timeoutSecs=120)

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(csvPathname, pathname1x)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)
        glm_doit(self,filename2x, pathname2x, timeoutSecs=60)

        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2x,pathname2x,pathname4x)
        
        print "Iterating 1 times on this last one for perf compare"
        for i in range(1):
            print "\nTrial #", i, "of", filename4x
            glm_doit(self,filename4x, pathname4x, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
