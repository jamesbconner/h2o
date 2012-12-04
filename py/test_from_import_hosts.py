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
        h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_importFolder_files(self):

        # assume the importFolder prefix is datasets, for now
        # just do the import folder once
        importFolderPath = "/home/0xdiag/datasets"
        node = h2o.nodes[0]
        importFolderResult = node.import_folder(importFolderPath)
        print importFolderResult

        def parseImportFolderFile(node=None, csvFilename=None):
            if not csvFilename: raise Exception('No csvFilename parameter in parseImportFolderFile')
            if not node: node = h2o.nodes[0]

            importFolderKey = csvFilename
            print "importFolderKey:", importFolderKey

            inspect = node.inspect(importFolderKey)
            print inspect
            parseKey = node.parse(key=importFolderKey, key2=csvFilename + ".hex")
            print parseKey
            return parseKey

        csvFilenameAll = [
            "billion_rows.csv.gz",
            "new-poker-hand.full.311M.txt.gz",
            "covtype20x.data",
            "covtype200x.data"
            ]
        csvFilenameList = random.sample(csvFilenameAll,1)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        timeoutSecs = 200
        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = parseImportFolderFile(csvFilename=csvFilename)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "parse result:", parseKey['Key']

            print "\n" + csvFilename
            start = time.time()
            RFview = h2o_cmd.runRFOnly(trees=1,parseKey=parseKey,timeoutSecs=timeoutSecs)

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

        # browseJsonHistoryAsUrl()

if __name__ == '__main__':
    h2o.unit_main()
