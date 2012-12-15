import unittest
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b
import random, sys, time

# randomBitVector
# randomFilter
# log
# makeEnum
# bug?
#        ['Result','<n>',' = slice(c.hex[','<col1>','],', '<row>', ')'],
exprList = [
        ['Result','<n>',' = colSwap(c.hex,', '<col1>', ',(c.hex[2]==0 ? 54321 : 54321))'],
        ['Result','<n>',' = c.hex[', '<col1>', ']'],
        ['Result','<n>',' = min(c.hex[', '<col1>', '])'],
        ['Result','<n>',' = max(c.hex[', '<col1>', ']) + Result'],
        ['Result','<n>',' = mean(c.hex[', '<col1>', ']) + Result'],
        ['Result','<n>',' = sum(c.hex[', '<col1>', ']) + Result'],
    ]

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        h2o.build_cloud(2)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_loop_random_exec_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(None, csvPathname, 'covtype.data', 'c.hex', 10000)
        print "\nParse key is:", parseKey['Key']

        h2b.browseTheCloud()
        # for trial in range(53):
        trial = 0
        while (trial < 100):
            for exprTemplate in exprList:
                # have to copy it to keep python from changing the original when I modify it below!
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

                # form the expression in a single string
                execExpr = ''.join(exprTemp)
                print "\nexecExpr:", execExpr

                start = time.time()

                exec_result = h2o_cmd.runExecOnly(parseKey=parseKey, 
                    timeoutSecs=70, Expr=execExpr)
                # FIX! race conditions. If json is done, does that mean you can inspect it??
                resultInspect = h2o.nodes[0].inspect('Result' + str(trial))
                # h2o.verboseprint(h2o.dump_json(resultInspect))

                print(h2o.dump_json(exec_result))
                h2o.verboseprint(h2o.dump_json(exec_result))

                # WARNING! we can't browse the Exec url history, since that will 
                # cause the Exec to execute again thru the browser..i.e. it has side effects
                # just look at the last inspect, which should be the resultInspect!
                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")

                # FIX! I suppose we have the problem of stdout/stderr not having flushed?
                # should hook in some way of flushing the remote node stdout/stderr
                if (h2o.check_sandbox_for_errors()):
                    raise Exception("Found errors in sandbox stdout or stderr, on trial #%s." % trial)

                print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'
                print "Trial #", trial, "completed\n"

                # use the result as the next thing to work on? (copy over)
                parseKey['Key'] = exec_result['ResultKey']
 

if __name__ == '__main__':
    h2o.unit_main()
