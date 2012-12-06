import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import time
import random


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_importFolder_files(self):

        # assume the importFolder prefix is datasets, for now
        # just do the import folder once
        importFolderPath = "/home/0xdiag/datasets"
        importFolderPath = "/home/hduser/hdfs_datasets"
        node = h2o.nodes[0]
        importFolderResult = node.import_folder(importFolderPath)
        h2o.dump_json(importFolderResult)

        def parseImportFolderFile(node=None, csvFilename=None):
            if not csvFilename: raise Exception('parseImportFolderFile: No csvFilename')
            if not node: node = h2o.nodes[0]

            csvPathnameForH2O = "nfs:/" + importFolderPath + "/" + csvFilename
            # hmm..are we required to inspect before parse? would think not, but let's look

            # inspect = node.inspect(csvPathnameForH2O)
            # print "\nInspect file result:", inspect

            # We like the short parse key2 name. 
            # We don't drop anything from csvFilename, unlike H2O default
            print "Waiting for the slow parse of the file:", csvFilename
            parseKey = node.parse(key=csvPathnameForH2O, key2=csvFilename + '.hex')
            print "\nParse result:", parseKey
            return parseKey

        #    "covtype200x.data"
        csvFilenameAll = [
            "billion_rows.csv.gz",
            "covtype169x.data",
            "covtype.13x.shuffle.data",
            "3G_poker_shuffle"
            ]
        csvFilenameList = random.sample(csvFilenameAll,1)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = parseImportFolderFile(csvFilename=csvFilename)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "Parse result['Key']:", parseKey['Key']

            # We should be able to see the parse result?
            inspect = node.inspect(parseKey['Key'])

            print "\n" + csvFilename
            start = time.time()
            timeoutSecs = 2000
            # because of poker and the water.UDP.set3(UDP.java) issue..constrain depth to 25
            RFview = h2o_cmd.runRFOnly(trees=1,depth=25,parseKey=parseKey,timeoutSecs=timeoutSecs)

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
