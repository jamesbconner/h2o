import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

# keep two lists the same size
# best if prime relative to the # jvms (len(h2o.nodes))
initList = [
        ['Result0 = 0'],
        ['Result1 = 1'],
        ['Result2 = 2'],
        ['Result3 = 3'],
        ['Result4 = 4'],
        ['Result5 = 5'],
        ['Result6 = 6'],
        ['Result7 = 7'],
        ['Result8 = 8'],
        ['Result9 = 9'],
        ['Result10 = 10'],
    ]

# NOTE. the inc has to match the goback used below
goback = 4
exprList = [
        ['Result','<n>',' = Result','<m>',' + ' + str(goback)],
    ]

def fill_in_expr_template(exprTemp,n, m):
        for i,e in enumerate(exprTemp):
            if e == '<n>':
                exprTemp[i] = str(n)
            if e == '<m>':
                exprTemp[i] = str(m)

        # form the expression in a single string
        execExpr = ''.join(exprTemp)
        h2o.verboseprint("\nexecExpr:", execExpr)
        return execExpr

def exec_expr(node, execExpr, trial, resultKey="Result"):
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

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### if (h2o.check_sandbox_for_errors()):
        ###     raise Exception(
        ###     "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        ### print "Trial #", trial, "completed\n"
        # use the result as the next thing to work on? (copy over)
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

    def test_exec_rotating_inc(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for exprTemplate in initList:
            exprTemp = list(exprTemplate)
            execExpr = fill_in_expr_template(exprTemp, 0, "Result")
            execResult = exec_expr(h2o.nodes[0], execExpr, 0)
            ### print "\nexecResult:", execResult

        period = 7
        # start at result10, to allow goback of 10
        trial = 0
        while (trial < 200):
            for exprTemplate in exprList:
                # copy it to keep python from changing the original when I modify it below!
                exprTemp = list(exprTemplate)
                # for the first 100 trials: do each expression at node 0,
                # for the second 100 trials: do each expression at a random node, to facilate key movement
                # FIX! there's some problem with the initList not taking if rotated amongst nodes?
                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                ### print nodeX
                
                number = trial + 10
                execExpr = fill_in_expr_template(exprTemp, number%period, (number-goback)%period)
                execResultInspect = exec_expr(h2o.nodes[nodeX], execExpr, number,
                    resultKey="Result" + str(trial%period))
                # FIX! we should be able to compare result against Trial #? 
                # maybe divided by # len of the expresssion list
                ### print "\nexecResult:", execResultInspect

                columns = execResultInspect["cols"]
                columnsDict = columns[0]
                min = columnsDict["min"]

                print min, execExpr
                h2o.verboseprint("min: ", min, "trial:", trial)
                self.assertEqual(float(min), float(trial), 
                    'Although the memory model allows write atomicity to be violated,' +
                    'this test was passing with an assumption of multi-jvm write atomicity' + 
                    'Be interesting if ever fails. Can disable assertion if so, and run without check')

                sys.stdout.write('.')
                sys.stdout.flush()

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1


if __name__ == '__main__':
    h2o.unit_main()
