
import h2o, h2o_cmd, sys
import time, random
# Trying to share some functions useful for creating random exec expressions
# and executing them
# these lists are just included for example
if (1==0):
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


def fill_in_expr_template(exprTemp, colX, n, row, key2):
    # FIX! does this push col2 too far? past the output col?
    for i,e in enumerate(exprTemp):
        if e == '<col1>':
            exprTemp[i] = str(colX)
        if e == '<col2>':
            exprTemp[i] = str(colX+1)
        if e == '<n>':
            exprTemp[i] = str(n) 
        if e == '<n-1>':
            exprTemp[i] = str(n-1) # we start with trial=0, so n-1 is Result0
        if e == '<row>':
            exprTemp[i] = str(row)
        if e == '<keyX>':
            exprTemp[i] = key2

    # form the expression in a single string
    execExpr = ''.join(exprTemp)
    ### h2o.verboseprint("\nexecExpr:", execExpr)
    print "\nexecExpr:", execExpr
    return execExpr


def exec_expr(node, execExpr, resultKey="Result", timeoutSecs=10):
    start = time.time()
    resultExec = h2o_cmd.runExecOnly(node, Expr=execExpr, timeoutSecs=timeoutSecs)
    h2o.verboseprint(resultExec)
    h2o.verboseprint('exec took', time.time() - start, 'seconds')
    print 'exec took', time.time() - start, 'seconds'

    h2o.verboseprint("\nfirst look at the default Result key")
    defaultInspect = h2o_cmd.runInspect(None, "Result")
    h2o.verboseprint(h2o.dump_json(defaultInspect))

    h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
    resultInspect = h2o_cmd.runInspect(None, resultKey)
    h2o.verboseprint(h2o.dump_json(resultInspect))

    return resultInspect


def exec_zero_list(zeroList):
    # zero the list of Results using node[0]
    for exprTemplate in zeroList:
        exprTemp = list(exprTemplate)
        execExpr = fill_in_expr_template(exprTemp,0,0,0,"Result")
        execResult = exec_expr(h2o.nodes[0], execExpr, "Result")
        ### print "\nexecResult:", execResult

def exec_expr_list_rand(lenNodes, exprList, key2, 
    maxCol=54, maxRow=400000, maxTrials=100, timeoutSecs=10):

    for trial in range(maxTrials):
        for exprTemplate in exprList:
            # copy it to keep python from changing the original when I modify it below!
            exprTemp = list(exprTemplate)
            # do each expression at a random node, to facilate key movement
            if lenNodes is None:
                nodeX = 0
            else:
                nodeX = random.randint(0,lenNodes-1)

            colX = random.randint(1,maxCol)

            # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
            row = str(random.randint(1,maxRow))

            execExpr = fill_in_expr_template(exprTemp, colX, trial+1, row, key2)
            execResultInspect = exec_expr(h2o.nodes[nodeX], execExpr,
                "Result"+str(trial+1), timeoutSecs)
            ### print "\nexecResult:", execResultInspect

            columns = execResultInspect["columns"]
            columnsDict = columns.pop()
            min = columnsDict["min"]
            h2o.verboseprint("min: ", min, "trial:", trial)

            sys.stdout.write('.')
            sys.stdout.flush()

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            # slows things down to check every iteration, but good for isolation
            if (h2o.check_sandbox_for_errors()):
                raise Exception(
                    "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

            print "Trial #", trial, "completed\n"



def exec_expr_list_across_cols(lenNodes, exprList, key2, maxCol=54, timeoutSecs=10):
    colResultList = []
    for colX in range(maxCol):
        for exprTemplate in exprList:
            # copy it to keep python from changing the original when I modify it below!
            exprTemp = list(exprTemplate)

            # do each expression at a random node, to facilate key movement
            if lenNodes is None:
                nodeX = 0
            else:
                nodeX = random.randint(0,lenNodes-1)

            execExpr = fill_in_expr_template(exprTemp, colX, colX, 0, key2)
            execResultInspect = exec_expr(h2o.nodes[nodeX], execExpr,
                "Result"+str(colX), timeoutSecs)
            ### print "\nexecResult:", execResultInspect

            columns = execResultInspect["columns"]
            columnsDict = columns.pop()
            min = columnsDict["min"]

            h2o.verboseprint("min: ", min, "col:", colX)
            print "min: ", min, "col:", colX

            sys.stdout.write('.')
            sys.stdout.flush()

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            # slows things down to check every iteration, but good for isolation
            if (h2o.check_sandbox_for_errors()):
                raise Exception(
                    "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        print "Column #", colX, "completed\n"
        colResultList.append(min)

    return colResultList


