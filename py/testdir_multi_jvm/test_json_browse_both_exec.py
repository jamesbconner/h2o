import unittest
import random, sys, time, webbrowser
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b

# Result from exec is an interesting key because it changes shape depending on the operation
# it's hard to overwrite keys with other operations. so exec gives us that, which allows us
# to test key atomicity. (value plus size plus other aspects)
# Spin doing browser inspects while doing the key. play with whether python can multithread for
# us (python interpreter lock issues though)

# randomBitVector
# randomFilter
# log
# makeEnum
# bug?
#        ['Result','<n>',' = slice(c.hex[','<col1>','],', '<row>', ')'],
exprList = [
        ['Result1',' = c.hex[', '<col1>', ']'],
        ['Result2',' = min(c.hex[', '<col1>', '])'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = max(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = mean(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = sum(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = min(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = max(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = mean(c.hex[', '<col1>', ']) + Result2'],
        ['Result1',' = c.hex[', '<col1>', '] + Result1'],
        ['Result1',' = sum(c.hex[', '<col1>', ']) + Result2'],
    ]

inspectList = ['Result1', 'Result2']

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

        # 3 nodes so we can hit the inspect from different nodes
        global lenNodes
        lenNodes = 3
        h2o.build_cloud(lenNodes)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_loop_random_exec_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(None, csvPathname, 'covtype.data', 'c.hex', 10)
        print "\nParse key is:", parseKey['destination_key']

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
                        # in the range of covtype row #'s
                        exprTemp[i] = str(random.randint(1,400000))

                # form the expression in a single string
                execExpr = ''.join(exprTemp)
                randNode = random.randint(0,lenNodes-1)
                print "\nexecExpr:", execExpr, "on node", randNode

                start = time.time()

                # pick a random node to execute it on
                resultExec = h2o_cmd.runExecOnly(node=h2o.nodes[randNode], Expr=execExpr, timeoutSecs=5)
                h2o.verboseprint(h2o.dump_json(resultExec))
                # print(h2o.dump_json(resultExec))

                # FIX! race conditions. If json is done, does that mean you can inspect it??
                # wait until the 2nd iteration, which will guarantee both Result1 and Result2 exist
                if trial > 1:
                    inspectMe = random.choice(inspectList)
                    resultInspect = h2o.nodes[0].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                    resultInspect = h2o.nodes[1].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                    resultInspect = h2o.nodes[2].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                # FIX! if we race the browser doing the exec too..it shouldn't be a problem?
                # might be a bug?

                # WARNING! we can't browse the Exec url history, since that will 
                # cause the Exec to execute again thru the browser..i.e. it has side effects
                # just look at the last inspect, which should be the resultInspect!
                # h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                h2b.browseJsonHistoryAsUrlLastMatch("Exec")
                # url = "http://192.168.0.37:54321/Exec?Expr=Result3+%3D+c.hex%5B26%5D+%2B+Result1&Key=Result"
                # webbrowser.open_new_tab(url)

                # FIX! I suppose we have the problem of stdout/stderr not having flushed?
                # should hook in some way of flushing the remote node stdout/stderr
                if (h2o.check_sandbox_for_errors()):
                    raise Exception("Found errors in sandbox stdout or stderr, on trial #%s." % trial)

                print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'
                print "Trial #", trial, "completed\n"

                # use the result as the next thing to work on? (copy over)
                parseKey['Key'] = resultExec['ResultKey']
 

if __name__ == '__main__':
    h2o.unit_main()
