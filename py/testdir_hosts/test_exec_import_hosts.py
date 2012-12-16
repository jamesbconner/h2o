import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

# 'randomBitVector'
# 'randomFilter'
# 'log"
# 'makeEnum'
# bug?
# ['Result','<n>',' = slice(','<keyX>','[','<col1>','],', '<row>', ')'],
exprList = [
        ['Result','<n>',' = colSwap(','<keyX>',',', '<col1>', ',(','<keyX>','[2]==0 ? 54321 : 54321))'],
        ['Result','<n>',' = ','<keyX>','[', '<col1>', ']'],
        ['Result','<n>',' = min(','<keyX>','[', '<col1>', '])'],
        ['Result','<n>',' = max(','<keyX>','[', '<col1>', ']) + Result'],
        ['Result','<n>',' = mean(','<keyX>','[', '<col1>', ']) + Result'],
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
        timeoutSecs = 4000
        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        # Update: need unique key names apparently. can't overwrite prior parse output key?
        csvFilenameAll = [
            ("covtype.data", "c"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype20x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("covtype200x.data", "c200"),
            ("billion_rows.csv.gz", "b"),
            ]
        csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        h2b.browseTheCloud()

        cnum = 0
        for (csvFilename, key2) in csvFilenameList:
            cnum += 1
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2, timeoutSecs=3000)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "Parse result['Key']:", parseKey['Key']

            # We should be able to see the parse result?
            inspect = h2o.nodes[0].inspect(parseKey['Key'])

            print "\n" + csvFilename
            # for determinism, I guess we should spit out the seed?
            # for trial in range(53):
            trial = 0
            while (trial < 100):
                for exprTemplate in exprList:
                    # copy it to keep python from changing the original when I modify it below!
                    exprTemp = list(exprTemplate)
                    trial = trial + 1
                    colX = random.randint(1,54)

                    # replace any <col2> in the template
                    # FIX! does this push col2 too far? past the output col?
                    for i,e in enumerate(exprTemp):
                        if e == '<col1>':
                            exprTemp[i] = str(colX)
                        if e == '<col2>':
                            exprTemp[i] = str(colX+1)
                        if e == '<n>':
                            exprTemp[i] = str(trial)
                        if e == '<row>':
                            exprTemp[i] = str(random.randint(1,400000))
                        if e == '<keyX>':
                            exprTemp[i] = key2

                    # form the expression in a single string
                    execExpr = ''.join(exprTemp)
                    print "\nexecExpr:", execExpr

                    start = time.time()
                    exec_result = h2o_cmd.runExecOnly(Expr=execExpr,timeoutSecs=70)
                    resultInspect = h2o.nodes[0].inspect('Result' + str(trial))
                    # h2o.verboseprint(h2o.dump_json(resultInspect))

                    print(h2o.dump_json(exec_result))
                    h2o.verboseprint(h2o.dump_json(exec_result))

                    ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                    if (h2o.check_sandbox_for_errors()):
                        raise Exception(
                            "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

                    print "exec end on ", csvFilename, 'took', time.time() - start, 'seconds'
                    print "Trial #", trial, "completed\n"

                    # use the result as the next thing to work on? (copy over)
                    parseKey['Key'] = exec_result['ResultKey']

if __name__ == '__main__':
    h2o.unit_main()
