import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

# the shared exec expression creator and executor
import h2o_exec as h2e

zeroList = [
        ['Result0 = 0'],
]

# FIX! put these in 3?
# 'randomBitVector' ?? hardwire size to 19?
# 'randomFilter'
# 'log"
# do we have to restrict ourselves?
# 'makeEnum' (hardware the enum colum to col 53
# bug?
#        ['Result','<n>',' = makeEnum(','<keyX>','[53]) + Result', '<n-1>'],
#        ['Result','<n>',' = randomFilter(','<keyX>','[', '<col1>','],' ,'<row>', ')'],
exprList = [
        ['Result','<n>',' = randomBitVector(19,0) + Result', '<n-1>'],
        ['Result','<n>',' = log(','<keyX>','[', '<col1>', ']) + Result', '<n-1>'],
        ['Result','<n>',' = ',
            '<keyX>','[', '<col1>', '] + ',
            '<keyX>','[', '<col2>', '] + ',
            '<keyX>','[', '2', ']'
        ],

        ['Result','<n>',' = slice(','<keyX>','[','<col1>','],', '<row>', ')'],
        ['Result','<n>',' = colSwap(','<keyX>',',', '<col1>', ',(','<keyX>','[2]==0 ? 54321 : 54321))'],
        ['Result','<n>',' = ','<keyX>','[', '<col1>', ']'],
        ['Result','<n>',' = min(','<keyX>','[', '<col1>', '])'],
        ['Result','<n>',' = max(','<keyX>','[', '<col1>', ']) + Result', '<n-1>'],
        ['Result','<n>',' = mean(','<keyX>','[', '<col1>', ']) + Result', '<n-1>'],
        ['Result','<n>',' = sum(','<keyX>','[', '<col1>', ']) + Result'],
    ]

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_import_hosts(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = "/home/0xdiag/datasets"
        h2i.setupImportFolder(None, importFolderPath)

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameAll = [
            ("covtype.data", "cA", 5),
            ("covtype.data", "cB", 5),
            ("covtype20x.data", "cC", 50),
            ("covtype20x.data", "cD", 50),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (csvFilename, key2, timeoutSecs) in csvFilenameList:
            cnum += 1
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "Parse result['Key']:", parseKey['Key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['Key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # we use colX+1 so keep it to 53
            # we use makeEnum in this test...so timeout has to be bigger!
            h2e.exec_expr_list_rand(lenNodes, exprList, key2, 
                maxCol=53, maxRow=400000, maxTrials=100, timeoutSecs=(timeoutSecs))


if __name__ == '__main__':
    h2o.unit_main()
