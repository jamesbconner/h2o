import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

zeroList = [
        ['Result0 = 0'],
]

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
        ['Result','<n>',' = max(','<keyX>','[', '<col1>', ']) + Result', '<n-1>'],
        ['Result','<n>',' = mean(','<keyX>','[', '<col1>', ']) + Result', '<n-1>'],
        ['Result','<n>',' = sum(','<keyX>','[', '<col1>', ']) + Result'],
    ]

def fill_in_expr_template(exprTemp,colX,trial,row,key2):
        # FIX! does this push col2 too far? past the output col?
        for i,e in enumerate(exprTemp):
            if e == '<col1>':
                exprTemp[i] = str(colX)
            if e == '<col2>':
                exprTemp[i] = str(colX+1)
            if e == '<n>':
                exprTemp[i] = str(trial)
            if e == '<n-1>':
                exprTemp[i] = str(trial-1) # start with trial = 1. init Result0
            if e == '<row>':
                exprTemp[i] = str(row)
            if e == '<keyX>':
                exprTemp[i] = key2

        # form the expression in a single string
        execExpr = ''.join(exprTemp)
        ### h2o.verboseprint("\nexecExpr:", execExpr)
        print "\nexecExpr:", execExpr
        return execExpr

def exec_expr(node, execExpr, resultKey="Result"):
        start = time.time()
        resultExec = h2o_cmd.runExecOnly(node, Expr=execExpr, timeoutSecs=120)
        h2o.verboseprint(resultExec)
        h2o.verboseprint('exec took', time.time() - start, 'seconds')

        h2o.verboseprint("\nfirst look at the default Result key")
        defaultInspect = h2o.nodes[0].inspect("Result")
        h2o.verboseprint(h2o.dump_json(defaultInspect))

        h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
        resultInspect = h2o.nodes[0].inspect(resultKey)
        h2o.verboseprint(h2o.dump_json(resultInspect))

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### if (h2o.check_sandbox_for_errors()):
        ###     raise Exception(
        ###     "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        ### print "Trial #", trial, "completed\n"
        # use the result as the next thing to work on? (copy over)
        return resultInspect


def exec_zero_list(zeroList):
        # zero the list of Results using node[0]
        for exprTemplate in zeroList:
            exprTemp = list(exprTemplate)
            execExpr = fill_in_expr_template(exprTemp,0,0,0,"Result")
            execResult = exec_expr(h2o.nodes[0], execExpr, "Result")
            ### print "\nexecResult:", execResult

def exec_list_like_other_tests(exprList, lenNodes, csvFilename, key2):
        # start with trial = 1 because trial-1 is used to point to Result0 which must be initted
        trial = 1
        while (trial < 100):
            for exprTemplate in exprList:
                # copy it to keep python from changing the original when I modify it below!
                exprTemp = list(exprTemplate)
                # do each expression at a random node, to facilate key movement
                nodeX = random.randint(0,lenNodes-1)
                colX = random.randint(1,54)

                # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
                row = str(random.randint(1,400000))

                execExpr = fill_in_expr_template(exprTemp, colX, trial, row, key2)
                execResultInspect = exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey="Result"+str(trial))
                ### print "\nexecResult:", execResultInspect

                columns = execResultInspect["columns"]
                columnsDict = columns.pop()
                min = columnsDict["min"]
                h2o.verboseprint("min: ", min, "trial:", trial)

                ### self.assertEqual(float(min), float(trial),"what can we check here")

                sys.stdout.write('.')
                sys.stdout.flush()

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                # slows things down to check every iteration, but good for isolation
                if (h2o.check_sandbox_for_errors()):
                    raise Exception(
                        "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

                # use the result as the next thing to work on? (copy over)
                # FIX! ??? huh? this is key2?
                #### parseKey['Key'] = exec_result['ResultKey']

                print "Trial #", trial, "completed\n"
                trial += 1

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
        # replicating lines means they'll get reparsed. good! (but give new key names)

        if (1==1): 
            csvFilenameAll = [
                ("covtype20x.data", "c20"),
                ("covtype200x.data", "c200"),
                ("billion_rows.csv.gz", "b"),
                ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (csvFilename, key2) in csvFilenameList:
            cnum += 1
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, 
                csvFilename, importFolderPath, key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse TimeMS:', parseKey['TimeMS']
            print "Parse result['Key']:", parseKey['Key']

            # We should be able to see the parse result?
            inspect = h2o.nodes[0].inspect(parseKey['Key'])

            print "\n" + csvFilename
            exec_zero_list(zeroList)
            exec_list_like_other_tests(exprList, lenNodes, csvFilename, key2)


if __name__ == '__main__':
    h2o.unit_main()
