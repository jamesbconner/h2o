import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i
import time, random, copy

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_importFolder_GLM_bigger_and_bigger(self):
        # We don't drop anything from csvFilename, unlike H2O default
        # FIX! for local 0xdata, this will be different (/home/0xdiag/datasets)
        csvFilenameList = [
            'covtype200x.data',
            'covtype200x.data',
            'covtype.data',
            'covtype.data',
            'covtype20x.data',
            'covtype20x.data',
            ]

        # a browser window too, just because we can
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        firstglm= {}
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=2000)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "Parse result['Key']:", parseKey['Key']

            # We should be able to see the parse result?
            inspect = h2o.nodes[0].inspect(parseKey['Key'])
            print "\n" + csvFilename

            start = time.time()
            # can't pass lamba as kwarg because it's a python reserved word
            # FIX! just look at X=0:1 for speed, for now
            # xval=10, norm="L2", glm_lambda=1e-4,
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey,
                Y=54, X='0:53', norm="L2", xval=2, family="binomial", 
                timeoutSecs=2000)

            # different when xvalidation is used? No trainingErrorDetails?
            h2o.verboseprint("\nglm:", glm)
            print "GLM time", glm['time']

            coefficients = glm['coefficients']
            print "coefficients:", coefficients

            tsv = glm['trainingSetValidation']
            print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']

            print "glm end on ", csvFilename, 'took', time.time() - start, 'seconds'

            h2b.browseJsonHistoryAsUrlLastMatch("GLM")

            # compare this glm to last one. since the files are concatenations, 
            # the results should be similar? 10% of first is allowed delta
            def compareToFirstGlm(self, key, glm, firstglm):
                delta = .1 * float(firstglm[key])
                msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
                self.assertAlmostEqual(float(glm[key]), float(firstglm[key]), delta=delta, msg=msg)
                self.assertGreaterEqual(float(glm[key]), 0.0, key + " not >= 0.0 in current")

            if not firstglm:
                # dicts are references? Make a real copy
                firstglm = glm.copy()
                firsttsv = tsv.copy()
                firstcoefficients = coefficients.copy()
            else:
                compareToFirstGlm(self,'0',coefficients,firstcoefficients)
                compareToFirstGlm(self,'trainingSetErrorRate',tsv,firsttsv)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
