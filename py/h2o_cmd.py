import os, json, unittest, time, shutil, sys
import h2o

def runGLM(node=None,csvPathname=None,X="0",Y="1",timeoutSecs=30,retryDelaySecs=0.5):
    if not csvPathname: raise Exception('No file name for GLM specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname)
    parse = node.parse(put['keyHref'])
    glm = runGLMOnly(node=node, parseKey=parse,X=X,Y=Y,
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
    return glm

def runGLMOnly(node=None,parseKey=None,X="0",Y="1",timeoutSecs=30,retryDelaySecs=0.5):
    if not parseKey: raise Exception('No file name for GLM specified')
    if not node: node = h2o.nodes[0]
    # FIX! add something like stabilize in RF to check results, and also retry/timeout
    glm = node.GLM(parseKey['keyHref'],X=X,Y=Y)
    return glm

# You can change those on the URL line woth "&colA=77&colB=99"
# LinReg draws a line from a collection of points.  Only works if you have 2 or more points.
# will get NaNs if A/B is just one point.

def runLR(node=None,csvPathname=None,colA=0,colB=1,timeoutSecs=30,retryDelaySecs=0.5):
    if not csvPathname: raise Exception('No file name for LR specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname)
    parse = node.parse(put['keyHref'])
    runLROnly(node=node, parseKey=parse, 
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

def runLROnly(node=None,parseKey=None,colA=0,colB=1,timeoutSecs=30,retryDelaySecs=0.5):
    if not parseKey: raise Exception('No file name for LR specified')
    if not node: node = h2o.nodes[0]
    # FIX! add something like stabilize in RF to check results, and also retry/timeout
    lr = node.linear_reg(parseKey['keyHref'], colA, colB)

###     # we'll have to add something for LR.json to verify the LR results

# there are more RF parameters in **kwargs. see h2o.py
def runRF(node=None, csvPathname=None, trees=5, timeoutSecs=30, retryDelaySecs=2, **kwargs):
    if not csvPathname: raise Exception('No file name for RF specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname)
    parse = node.parse(put['keyHref'])
    rfView = runRFOnly(node=node, parseKey=parse, trees=trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, **kwargs)
    return(rfView)

# there are more RF parameters in **kwargs. see h2o.py
def runRFOnly(node=None, parseKey=None, trees=5, depth=30, 
        timeoutSecs=30, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for RF specified')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseKey that we should check?
    h2o.verboseprint("runRFOnly parseKey:",parseKey)
    keyHref = parseKey['keyHref']
    # FIX! CAN
    rf = node.random_forest(keyHref, trees, depth, **kwargs)

    # rf result json: 
    # this is the number of trees asked for
    # u'ntree': 6, 
    # this is the number of trees currently in the model (changes till == ntree)
    # u'modelSize: 6, 
    # u'class': 4
    # u'dataKey': u'...', 
    # u'treesKey': u'...', 
    # u'modelKey': u'model', 
    # u'dataKeyHref': u'...', 
    # u'treesKeyHref': u'...', 
    # u'modelKeyHref': u'____model'
    # u'h2o': u'/192.168.0.35:54321', 

    # FIX! check all of these somehow?
    dataKey      = rf['dataKey']
    modelKey     = rf['modelKey']
    treesKey     = rf['treesKey']

    # FIX! how come there's no .hex in these already? (there were in Parse.json? result?))
    dataKeyHref  = rf['dataKeyHref']
    modelKeyHref = rf['modelKeyHref']
    treesKeyHref = rf['treesKeyHref']
    # /ip:port of cloud (can't use h2o name)
    rfCloud = rf['h2o']
    # not goal # of trees?, or current that RF is out?. trees is the goal?
    ntree = rf['ntree']
    whatIsThis= rf['class']

    # this expects the response to match the number of trees you told it to do
    # FIX! temporary hack to allow nodes*trees to be a legal final response also
    # FIX! is ntree the right thing in the rf view result?

    def test(n):
        return(n.random_forest_view(dataKeyHref,modelKeyHref,treesKeyHref)['modelSize']==trees)

    node.stabilize(
            test,
            'random forest reporting %d trees' % trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

    rfView = node.random_forest_view(dataKeyHref,modelKeyHref,treesKeyHref)
    modelSize = rfView['modelSize']
    # u'confusionKey': u'...', 
    # u'confusionKeyHref': u'____model'
    confusionKey = rfView['confusionKey']
    confusionKeyHref = rfView['confusionKeyHref']

    # FIX! how am I supposed to verify results, or get results/
    # touch all these just to do something
    cmInspect = node.inspect(confusionKeyHref)
    treeInspect = node.inspect(treesKeyHref)
    modelInspect = node.inspect(modelKeyHref)
    dataInspect = node.inspect(dataKeyHref)

    return(rfView)
