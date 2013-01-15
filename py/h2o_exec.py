
import h2o, h2o_cmd, sys
import time, random, re
# Trying to share some functions useful for creating random exec expressions
# and executing them
# these lists are just included for example
if (1==0):
    zeroList = [
            'Result0 = 0',
    ]

    # 'randomBitVector'
    # 'randomFilter'
    # 'log"
    # 'makeEnum'
    # bug?
    # 'Result<n> = slice(<keyX>[<col1>],<row>)',
    exprList = [
            'Result<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
            'Result<n> = <keyX>[<col1>]',
            'Result<n> = min(<keyX>[<col1>])',
            'Result<n> = max(<keyX>[<col1>]) + Result<n-1>',
            'Result<n> = mean(<keyX>[<col1>]) + Result<n-1>',
            'Result<n> = sum(<keyX>[<col1>]) + Result.hex',
        ]

def checkForBadFP(min):
    if ('built-in' in str(min)) or ('Built-in' in str(min)):
        raise Exception("Weird 'built-in' string in inspected min (proxy for scalar result): %s" % str(min))
    if 'Infinity' in str(min):
        raise Exception("Infinity in inspected min (proxy for scalar result) can't be good: %s" % str(min))
    if 'NaN' in str(min):
        raise Exception("NaNin inspected min (proxy for scalar result)  can't be good: %s" % str(min))

def checkScalarResult(resultInspect, resultKey):
    # make the common problems easier to debug
    h2o.verboseprint(h2o.dump_json(resultInspect))
    emsg = None
    while(True):
        if 'type' not in resultInspect:
            emsg = "'type' missing. Look at the json just printed"
            break 
        type = resultInspect["type"]
        if 'unparsed' in type:
            emsg = "'cols' has 'type' of unparsed. Look at the json just printed"
            break 

        if 'cols' not in resultInspect:
            emsg = "Inspect response: 'cols' missing. Look at the json just printed"
            break 

        cols = resultInspect["cols"]
        if not isinstance(cols, list):
            emsg = "'cols' is supposed to be a one element list. Look at the json just printed"
            break 
        if 'unknown' in cols:
            emsg = "'cols' has 'unknown'. Look at the json just printed"
            break 
        colsDict = cols[0]

        if 'min' not in colsDict:
            emsg = "'cols' doesn't have 'min'. Look at the json just printed"
            break 
        min = colsDict["min"]

        if 'built-in' in colsDict:
            emsg = "Some weird 'built-in' response. Look at the json just printed"
            break 
        break

    if emsg is not None:
        print "\nKey: '" + resultKey + "' being inspected:\n", h2o.dump_json(resultInspect)
        sys.stdout.flush()
        raise Exception("Inspect problem:" + emsg)

    checkForBadFP(min)
    return min

def fill_in_expr_template(exprTemplate, colX, n, row, key2):
    # FIX! does this push col2 too far? past the output col?
    # just a string? 
    execExpr = exprTemplate
    execExpr = re.sub('<col1>',str(colX),execExpr)
    execExpr = re.sub('<col2>',str(colX+1),execExpr)
    execExpr = re.sub('<n>',str(n),execExpr)
    execExpr = re.sub('<n-1>',str(n-1),execExpr)
    execExpr = re.sub('<row>',str(row),execExpr)
    execExpr = re.sub('<keyX>',str(key2),execExpr)
    ### h2o.verboseprint("\nexecExpr:", execExpr)
    print "execExpr:", execExpr
    return execExpr


def exec_expr(node, execExpr, resultKey="Result.hex", timeoutSecs=10):
    start = time.time()
    resultExec = h2o_cmd.runExecOnly(node, expression=execExpr, timeoutSecs=timeoutSecs)
    h2o.verboseprint(resultExec)
    h2o.verboseprint('exec took', time.time() - start, 'seconds')
    ### print 'exec took', time.time() - start, 'seconds'

    # normal
    if 1==1:
        h2o.verboseprint("\nfirst look at the default Result key")
        defaultInspect = h2o_cmd.runInspect(None, "Result.hex")
        min = checkScalarResult(defaultInspect, "Result.hex")

        h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
        resultInspect = h2o_cmd.runInspect(None, resultKey)
        min = checkScalarResult(resultInspect, resultKey)

    # for debug
    # for debug! dummy assign because of removed inspect above
    else:
        resultInspect = {u'rows': 19, u'rowsize': 8, u'cols': 1, u'key': u'Result1', u'type': u'ary', u'columns': [{u'scale': u'\x01', u'off': 0, u'name': u'0', u'min': u'0.0', u'max': u'0.0', u'badat': u'19', u'base': 0, u'var': u'0.0', u'mean': u'0.0', u'type': u'float', u'size': 8}], u'size': 152}

    return resultInspect


def exec_zero_list(zeroList):
    # zero the list of Results using node[0]
    for exprTemplate in zeroList:
        execExpr = fill_in_expr_template(exprTemplate,0,0,0,"Result.hex")
        execResult = exec_expr(h2o.nodes[0], execExpr, "Result.hex")
        ### print "\nexecResult:", execResult


def exec_expr_list_rand(lenNodes, exprList, key2, 
    minCol=0, maxCol=54, minRow=1, maxRow=400000, maxTrials=200, timeoutSecs=10):

    trial = 0
    while trial < maxTrials: 
        exprTemplate = random.choice(exprList)

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

        execExpr = fill_in_expr_template(exprTemplate, colX, ((trial+1)%4)+1, row, key2)
        execResultInspect = exec_expr(h2o.nodes[execNode], execExpr, "Result.hex", timeoutSecs)
        ### print "\nexecResult:", execResultInspect

        min = checkScalarResult(execResultInspect, "Result.hex")

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
    minCol=0, maxCol=54, timeoutSecs=10, incrementingResult=True):
    colResultList = []
    for colX in range(minCol, maxCol):
        for exprTemplate in exprList:
            # do each expression at a random node, to facilate key movement
            # UPDATE: all execs are to a single node. No mixed node streams
            # eliminates some store/store race conditions that caused problems.
            # always go to node 0 (forever?)
            if lenNodes is None:
                execNode = 0
            else:
                ### execNode = random.randint(0,lenNodes-1)
                ### print execNode
                execNode = 0

            execExpr = fill_in_expr_template(exprTemplate, colX, colX, 0, key2)
            if incrementingResult: # the Result<col> pattern
                resultKey = "Result"+str(colX)
            else: # assume it's a re-assign to self
                resultKey = key2

            execResultInspect = exec_expr(h2o.nodes[execNode], execExpr,
                resultKey, timeoutSecs)
            ### print "\nexecResult:", execResultInspect

            # min is keyword. shouldn't use.
            if incrementingResult: # a col will have a single min
                min = checkScalarResult(execResultInspect, resultKey)
                h2o.verboseprint("min: ", min, "col:", colX)
                print "min: ", min, "col:", colX
            else:
                min = None

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


