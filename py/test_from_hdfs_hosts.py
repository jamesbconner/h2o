import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i
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
        # larger set in my local dir
        csvFilenameAll = [
            "3G_poker_shuffle",
            "TEST-poker1000.csv",
            "allstate_claim_prediction_train_set.zip",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            "bestbuy_test.csv",
            "bestbuy_train.csv",
            "billion_rows.csv.gz",
            "covtype.13x.data",
            "covtype.13x.shuffle.data",
            "covtype.169x.data",
            "covtype.4x.shuffle.data",
            "covtype.data",
            "covtype4x.shuffle.data",
            "hhp.unbalanced.012.1x11.data.gz",
            "hhp.unbalanced.012.data.gz",
            "hhp.unbalanced.data.gz",
            "hhp2.os.noisy.0_1.data",
            "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            "leads.csv",
            "poker-hand.1244M.shuffled311M.full.txt",
            "poker_c1s1_testing_refresh.csv",
            "prostate_2g.csv",
            "prostate_long.csv.gz",
            "prostate_long_1G.csv",
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
            parseKey = h2i.parseHdfsFile(csvFilename=csvFilename, timeoutSecs=1000)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "parse result:", parseKey['Key']
            # I use this if i want the larger set in my localdir

            print "\n" + csvFilename
            start = time.time()
            RFview = h2o_cmd.runRFOnly(trees=1,parseKey=parseKey,timeoutSecs=2000)
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
