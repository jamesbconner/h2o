
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

def checkForBadFP(min):
    if 'Infinity' in str(min):
        raise Exception("Infinity in inspected min (proxy for scalar result) can't be good: %s" % str(min))
    if 'NaN' in str(min):
        raise Exception("NaNin inspected min (proxy for scalar result)  can't be good: %s" % str(min))

def checkScalarResult(resultInspect):
    # make the common problems easier to debug
    h2o.verboseprint(h2o.dump_json(resultInspect))
    if 'cols' not in resultInspect:
        print "\nSome result being inspected. Use -v for more:\n", h2o.dump_json(resultInspect)
        raise Exception("Inspect response: 'cols' missing. Look at the json just printed")
    columns = resultInspect["cols"]

    if not isinstance(columns, list):
        print "\nSome result being inspected. Use -v for more:\n", h2o.dump_json(resultInspect)
        raise Exception("Inspect response: 'cols' is supposed to be a one element list. Look at the json just printed")
    columnsDict = columns[0]

    if 'min' not in columnsDict:
        print "\nSome result being inspected. Use -v for more:\n", h2o.dump_json(resultInspect)
        raise Exception("Inspect response: 'cols' doesn't have 'min'. Look at the json just printed")
    min = columnsDict["min"]
    checkForBadFP(min)

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
            exprTemp[i] = str(n-1) # we start with trial=1, so n-1 is Result0
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
    ## print "HACK! do exec twice to avoid the race in shape/result against the next inspect"
    ## good for testing store/store races? should sequence thru different nodes too 
    ### resultExec = h2o_cmd.runExecOnly(node, Expr=execExpr, timeoutSecs=timeoutSecs)
    h2o.verboseprint(resultExec)
    h2o.verboseprint('exec took', time.time() - start, 'seconds')
    ### print 'exec took', time.time() - start, 'seconds'

    # normal
    if 1==1:
        h2o.verboseprint("\nfirst look at the default Result key")
        defaultInspect = h2o_cmd.runInspect(None, "Result")
        checkScalarResult(defaultInspect)

        h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
        resultInspect = h2o_cmd.runInspect(None, resultKey)
        checkScalarResult(resultInspect)

    # for debug
    # for debug! dummy assign because of removed inspect above
    else:
        resultInspect = {u'rows': 19, u'rowsize': 8, u'cols': 1, u'key': u'Result1', u'type': u'ary', u'columns': [{u'scale': u'\x01', u'off': 0, u'name': u'0', u'min': u'0.0', u'max': u'0.0', u'badat': u'19', u'base': 0, u'var': u'0.0', u'mean': u'0.0', u'type': u'float', u'size': 8}], u'size': 152}

    return resultInspect


def exec_zero_list(zeroList):
    # zero the list of Results using node[0]
    for exprTemplate in zeroList:
        exprTemp = list(exprTemplate)
        execExpr = fill_in_expr_template(exprTemp,0,0,0,"Result")
        execResult = exec_expr(h2o.nodes[0], execExpr, "Result")
        ### print "\nexecResult:", execResult


def exec_expr_list_rand(lenNodes, exprList, key2, 
    minCol=0, maxCol=54, minRow=1, maxRow=400000, maxTrials=200, timeoutSecs=10):

    trial = 0
    while trial < maxTrials: 
        exprTemplate = random.choice(exprList)

        # copy it to keep python from changing the original when I modify it below!
        exprTemp = list(exprTemplate)
        # UPDATE: all execs are to a single node. No mixed node streams
        # eliminates some store/store race conditions that caused problems.
        # always go to node 0 (forever?)
        if lenNodes is None:
            execNode = 0
        else:
            # execNode = random.randint(0,lenNodes-1)
            execNode = 0
        ## print "execNode:", execNode

        colX = random.randint(minCol,maxCol)

        # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
        row = str(random.randint(minRow,maxRow))

        execExpr = fill_in_expr_template(exprTemp, colX, ((trial+1)%4)+1, row, key2)
        execResultInspect = exec_expr(h2o.nodes[execNode], execExpr,
            "Result", timeoutSecs)
        ### print "\nexecResult:", execResultInspect

        checkScalarResult(execResultInspect)

        sys.stdout.write('.')
        sys.stdout.flush()

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        # slows things down to check every iteration, but good for isolation
        if (h2o.check_sandbox_for_errors()):
            raise Exception(
                "Found errors in sandbox stdout or stderr, on trial #%s." % trial)
        trial += 1
        print "Trial #", trial, "completed\n"

def exec_expr_list_across_cols(lenNodes, exprList, key2, 
    minCol=0, maxCol=54, timeoutSecs=10):
    colResultList = []
    for colX in range(minCol, maxCol):
        for exprTemplate in exprList:
            # copy it to keep python from changing the original when I modify it below!
            exprTemp = list(exprTemplate)

            # do each expression at a random node, to facilate key movement
            # UPDATE: all execs are to a single node. No mixed node streams
            # eliminates some store/store race conditions that caused problems.
            # always go to node 0 (forever?)
            if lenNodes is None:
                execNode = 0
            else:
                # execNode = random.randint(0,lenNodes-1)
                execNode = 0

            print execNode
            execExpr = fill_in_expr_template(exprTemp, colX, colX, 0, key2)
            execResultInspect = exec_expr(h2o.nodes[execNode], execExpr,
                "Result"+str(colX), timeoutSecs)
            ### print "\nexecResult:", execResultInspect

            checkScalarResult(execResultInspect)
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


