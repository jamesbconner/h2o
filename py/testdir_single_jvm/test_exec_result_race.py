import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

initList = [
        ['Result = 0'],
        ['Result = 1'],
        ['Result = 2'],
        ['Result = 3'],
        ['Result = 4'],
        ['Result = 5'],
        ['Result = 6'],
        ['Result = 7'],
        ['Result = 8'],
        ['Result = 9'],
        ['Result = 10'],
    ]

exprList = [
        ['Result = Result + 1'],
    ]

def exec_expr(node, execExpr, trial, resultKey="Result"):
        start = time.time()
        resultExec = h2o_cmd.runExecOnly(node, Expr=execExpr, timeoutSecs=70)
        h2o.verboseprint(resultExec)
        h2o.verboseprint('exec took', time.time() - start, 'seconds')

        h2o.verboseprint("\nfirst look at the default Result key")
        defaultInspect = h2o.nodes[0].inspect("Result")
        h2o.verboseprint(h2o.dump_json(defaultInspect))

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### if (h2o.check_sandbox_for_errors()):
        ###     raise Exception(
        ###     "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        return defaultInspect

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_result_race(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for execExpr in initList:
            execResult = exec_expr(h2o.nodes[0], execExpr, 0)
            ### print "\nexecResult:", execResult

        trial = 0
        while (trial < 200):
            for exprExpr in exprList:
                # for the first 100 trials: do each expression at node 0,
                # for the second 100 trials: do each expression at a random node, to facilate key movement
                # FIX! there's some problem with the initList not taking if rotated amongst nodes?
                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                
                execResultInspect = exec_expr(h2o.nodes[nodeX], execExpr, 0, resultKey="Result")
                columns = execResultInspect["cols"]
                columnsDict = columns[0]
                min = columnsDict["min"]

                print min, execExpr
                h2o.verboseprint("min: ", min, "trial:", trial)
                sys.stdout.write('.')
                sys.stdout.flush()

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1


if __name__ == '__main__':
    h2o.unit_main()
