import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import time


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # h2o_hosts.build_cloud_with_hosts()
        # single jvm on local machine
        h2o.build_cloud(1)

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

        timeoutSecs = 200
        # save the first, for all comparisions, to avoid slow drift with each iteration
        firstglm = {}
        for csvFilename in csvFilenameList:
            csvPathname = h2o.find_file('smalldata/' + csvFilename)
            # I use this if i want the larger set in my localdir
            # csvPathname = h2o.find_file('/home/kevin/scikit/datasets/logreg/' + csvFilename)

            print "\n" + csvPathname

            start = time.time()
            ### FIX! add some expected result checking
            # can't pass lamba as kwarg because it's a python reserved word
            glm = h2o_cmd.runGLM(csvPathname=csvPathname, Y=7, family="binomial", 
                xval=10, norm="L1", glm_lambda=1e-4,
                timeoutSecs=timeoutSecs)

            # different when xvalidation is used? No trainingErrorDetails?
            h2o.verboseprint("\nglm:", glm)
            if 'warnings' in glm:
                print "\nwarnings:", glm['warnings']

            print "GLM time", glm['time']
            print "coefficients:", glm['coefficients']
            print glm

            tsv = glm['trainingSetValidation']
            print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']
            # ted = glm['trainingErrorDetails']

            # print "trueNegative:", ted['trueNegative']
            # print "truePositive:", ted['truePositive']
            # print "falseNegative:", ted['falseNegative']
            # print "falsePositive:", ted['falsePositive']

            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            # maybe we can see the GLM results in a browser?
            # FIX! does it recompute it??
            h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            # wait in case it recomputes it
            time.sleep(10)

            # compare this glm to the last one. since the files are concatenations, the results
            # should be similar?

            def glmCompareToFirst(self,key,glm,firstglm):
                # 10% of first is allowed delta
                delta = .1 * float(firstglm[key])
                msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
                self.assertAlmostEqual(float(glm[key]), float(firstglm[key]), delta=delta, msg=msg)
                self.assertGreaterEqual(float(glm[key]), 0.0, key + " not >= 0.0 in current")

            if firstglm:
                glmCompareToFirst(self, 'trainingSetErrorRate', tsv, firstglm)
            else:
                # dicts are references? Make a real copy
                firstglm['trainingSetErrorRate'] = tsv['trainingSetErrorRate']

            sys.stdout.write('.')
            sys.stdout.flush() 

        # browseJsonHistoryAsUrl()

if __name__ == '__main__':
    h2o.unit_main()
