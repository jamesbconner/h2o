import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

# keep two lists the same size
# best if prime relative to the # jvms (len(h2o.nodes))
period = 11
initList = [
        ['Result0 = -1'],
        ['Result1 = Result + 1'],
        ['Result2 = Result + 1'],
        ['Result3 = Result + 1'],
        ['Result4 = 3'],
        ['Result5 = 4'],
        ['Result6 = 5'],
        ['Result7 = 6'],
        ['Result8 = 7'],
        ['Result9 = 8'],
        ['Result10 = 9'],
    ]

def my_exec_expr(node, execExpr, trial, resultKey="Result"):
        start = time.time()
        resultExec = h2o_cmd.runExecOnly(node, Expr=execExpr, timeoutSecs=70)
        h2o.verboseprint(resultExec)
        h2o.verboseprint('exec took', time.time() - start, 'seconds')

        h2o.verboseprint("\nfirst look at the default Result key")
        defaultInspect = h2o.nodes[0].inspect("Result")
        h2o.verboseprint(h2o.dump_json(defaultInspect))

        h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
        resultInspect = h2o.nodes[0].inspect(resultKey)
        h2o.verboseprint(h2o.dump_json(resultInspect))

        return resultInspect

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
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_assign(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        trial = 0
        while (trial < 200):
            for exprTemplate in initList:
                execExpr = list(exprTemplate)
                # always a one node stream. shouldn't fail
                nodeX = 0
                resultKey="Result" + str(trial%period)
                execResultInspect = my_exec_expr(h2o.nodes[nodeX], execExpr, 0, resultKey=resultKey)
                ### print "\nexecResult:", execResultInspect
                min = h2e.checkScalarResult(execResultInspect,resultKey)

                print "trial: #" + str(trial), min, execExpr
                h2o.verboseprint("min: ", min, "trial:", trial)
                self.assertEqual(float(min), float((trial % period) - 1), 
                    "exec constant assigns don't seem to be getting done and visible to Inspect")

                sys.stdout.write('.')
                sys.stdout.flush()
                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1

if __name__ == '__main__':
    h2o.unit_main()
