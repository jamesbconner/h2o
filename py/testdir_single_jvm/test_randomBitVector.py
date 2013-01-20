import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

exprList = [
        # keep it less than 6 so we can see all the values with an inspect?
        (6,2,4, 'a.hex = randomBitVector(6,2,9)'),
    ]

def exec_expr(node, execExpr, trial, resultKey="Result.hex"):
        start = time.time()
        resultExec = h2o_cmd.runExecOnly(node, expression=execExpr, timeoutSecs=70)
        print (resultExec)
        h2o.verboseprint('exec took', time.time() - start, 'seconds')

        print ("\nfirst look at the default Result key")
        defaultInspect = h2o_cmd.runInspect(None,"Result.hex")
        print (h2o.dump_json(defaultInspect))

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### if (h2o.check_sandbox_for_errors()):
        ###     raise Exception(
        ###     "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        return defaultInspect

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_randomBitVector(self):
        h2b.browseTheCloud()

        trial = 0
        for (expectedRows, expectedOnes, expectedZeroes, execExpr) in exprList:
            execResultInspect = exec_expr(h2o.nodes[0], execExpr, 0, resultKey="Result.hex")
            columns = execResultInspect["cols"]
            columnsDict = columns[0]

            num_columns = execResultInspect['num_columns']
            num_rows = execResultInspect['num_rows']
            row_size = execResultInspect['row_size']

            # FIX! is this right?
            if (num_columns != 1):
                raise Exception("Wrong num_columns in randomBitVector result.  expected: %d, actual: %d" %\
                    (1, num_columns))

            if (num_rows != expectedRows):
                raise Exception("Wrong num_rows in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedRows, num_rows))

            if (row_size != 1):
                raise Exception("Wrong row_size in randomBitVector result.  expected: %d, actual: %d" %\
                    (1, row_size))

            # count the zeroes and ones in the created data
            actualZeroes = 0
            actualOnes = 0
            for i in range (expectedRows):
                value = columnsDict[str(i)]
                if value == 0:
                    actualZeroes += 1
                elif value == 1:
                    actualOnes += 1
                else:
                    raise Exception("Bad value in cols dict of randomBitVector result. key: %s, value: %s" % (i, value))

            if (actualOnes != expectedOnes):
                raise Exception("Wrong number of 1's in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedOnes, actualOnes))
                
            if (actualZeroes != expectedZeroes):
                raise Exception("Wrong number of 0's in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedZeroes, actualZeroes))

            min = columnsDict["min"]
            max = columnsDict["max"]
            mean = columnsDict["max"]
            print min, max, mean, execExpr
            sys.stdout.write('.')
            sys.stdout.flush()

            h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
