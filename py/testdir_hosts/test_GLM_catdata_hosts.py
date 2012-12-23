import os, json, unittest, time, shutil, sys
import copy
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm
import h2o_hosts
import h2o_browse as h2b
import time


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_100kx7cat(self):
        # larger set in my local dir
        csvFilenameList = [
            "1_100kx7_logreg.data",
            "2_100kx7_logreg.data",
            "4_100kx7_logreg.data",
            "8_100kx7_logreg.data",
            "16_100kx7_logreg.data"
        ]
        # these are still in /home/kevin/scikit/datasets/logreg
        # FIX! just two for now..
        csvFilenameList = [
            "1_100kx7_logreg.data.gz",
            "2_100kx7_logreg.data.gz"
        ]

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        # save the first, for all comparisions, to avoid slow drift with each iteration
        validations1 = {}
        for csvFilename in csvFilenameList:
            csvPathname = h2o.find_file('smalldata/' + csvFilename)
            # I use this if i want the larger set in my localdir
            # csvPathname = h2o.find_file('/home/kevin/scikit/datasets/logreg/' + csvFilename)

            print "\n" + csvPathname

            start = time.time()
            # can't pass lamba as kwarg because it's a python reserved word
            kwargs = {'Y': 7, 'glm_notX': '1:52', 'family': "binomial", 'xval': 10, 
                'norm': "L1", 'glm_lambda': 1e-4}
            timeoutSecs = 200
            glm = h2o_cmd.runGLM(csvPathname=csvPathname, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 27, **kwargs)


            # different when xvalidation is used? No trainingErrorDetails?
            h2o.verboseprint("\nglm:", glm)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            # maybe we can see the GLM results in a browser?
            # FIX! does it recompute it??
            h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            # wait in case it recomputes it
            time.sleep(10)

            # compare this glm to the last one. since the files are concatenations, the results
            # should be similar?

            GLMModel = glm['GLMModel']
            validationsList = GLMModel['validations']
            validations = validationsList.pop()
            # validations['err']

            if validations1:
                h2o_glm.glmCompareToFirst(self, 'err', validations, validations1)
            else:
                validations1 = deepcopy(validations)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
