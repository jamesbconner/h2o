import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import time
import random


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # h2o_hosts.build_cloud_with_hosts()
        # single jvm on local machine
        h2o_hosts.build_cloud_with_hosts(1,use_hdfs=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_hdfs_files(self):

        # FIX! can update this to parse from local dir also (import keys from folder?)
        # but everyone needs to have a copy then
        def parseHdfsFile(node=None, csvFilename=None):
            if not csvFilename: raise Exception('No csvFilename parameter in inspectHdfsFile')
            if not node: node = h2o.nodes[0]

            # assume the hdfs prefix is datasets, for now
            hdfsPrefix = "hdfs://"
            # 25s show up?
            # hdfsPrefix = "hdfs%3A%2F%2F"

            hdfsKey = hdfsPrefix + csvFilename
            print "hdfsKey:", hdfsKey

            # FIX! getting H2O HPE?
            inspect = node.inspect(hdfsKey)
            print inspect
            parseKey = node.parse(key=hdfsKey, key2=csvFilename + ".hex")
            print parseKey
            return parseKey

        # larger set in my local dir
        csvFilenameAll = [
            "TEST-poker1000.csv",
            "covtype.4x.shuffle.data",
            "covtype.13x.shuffle.data",
            "prostate_long_1G.csv",
            "hhp.unbalanced.data.gz",
            "3G_poker_shuffle",
            "allstate_claim_prediction_train_set.zip",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            "bestbuy_test.csv",
            "bestbuy_train.csv",
            "billion_rows.csv.gz",
            "covtype.169x.data",
            "covtype.13x.data",
            "covtype.data",
            "covtype4x.shuffle.data",
            "hhp.unbalanced.012.1x11.data.gz",
            "hhp.unbalanced.012.data.gz",
            "hhp2.os.noisy.0_1.data",
            "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            "leads.csv",
            "ph.full.933M.txt",
            "poker-hand.1244M.shuffled311M.full.txt",
            "poker_c1s1_testing_refresh.csv",
            "prostate_2g.csv",
            "prostate_long.csv.gz",
        ]

        # pick 8 randomly!
        csvFilenameList = random.sample(csvFilenameAll,8)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        timeoutSecs = 200
        # save the first, for all comparisions, to avoid slow drift with each iteration
        firstglm = {}
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            parseKey = parseHdfsFile(csvFilename=csvFilename)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "parse result:", parseKey['Key']
            # I use this if i want the larger set in my localdir

            print "\n" + csvFilename
            start = time.time()
            ### FIX! add some expected result checking
            # can't pass lamba as kwarg because it's a python reserved word
            RFview = h2o_cmd.runRFOnly(trees=1,parseKey=parseKey,timeoutSecs=timeoutSecs)

#             glm = h2o_cmd.runGLMOnly(parseKey=parseKey, Y=7, family="binomial", 
#                 xval=10, norm="L1", glm_lambda=1e-4,
#                 timeoutSecs=timeoutSecs)
# 
#             h2o.verboseprint("\nglm:", glm)
# 
#             print "errRate:", glm['errRate']
#             print "trueNegative:", glm['trueNegative']
#             print "truePositive:", glm['truePositive']
#             print "falseNegative:", glm['falseNegative']
#             print "falsePositive:", glm['falsePositive']
#             print "coefficients:", glm['coefficients']
#             print "glm end on ", parseKey, 'took', time.time() - start, 'seconds'
# 
#             # maybe we can see the GLM results in a browser?
#             # FIX! does it recompute it??
#             h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            # compare this glm to the last one. since the files are concatenations, the results
            # should be similar?

            def glmCompareToFirst(self,key):
                # 10% of first is allowed delta
                delta = .1 * float(glm[key])
                msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
                self.assertAlmostEqual(float(glm[key]), float(firstglm[key]), delta=delta, msg=msg);
                self.assertGreaterEqual(float(glm[key]), 0.0, key + " not >= 0.0 in current")


#             if firstglm:
#                 glmCompareToFirst(self,'errRate')
#                 glmCompareToFirst(self,'trueNegative')
#                 glmCompareToFirst(self,'truePositive')
#                 glmCompareToFirst(self,'falseNegative')
#                 glmCompareToFirst(self,'falsePositive')
#             else:
#                 # dicts are references? Make a real copy
#                 firstglm = glm.copy()

            sys.stdout.write('.')
            sys.stdout.flush() 

        # browseJsonHistoryAsUrl()

if __name__ == '__main__':
    h2o.unit_main()
