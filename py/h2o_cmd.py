import os, json, unittest, time, shutil, sys
import h2o
import h2o_browse as h2b

def parseFile(node=None, csvPathname=None, key=None, key2=None, timeoutSecs=20):
    if not csvPathname: raise Exception('No file name specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname, key=key)
    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = put['key'] + '.hex'
    else:
        myKey2 = key2
    return node.parse(put['key'], myKey2, timeoutSecs)

def runInspect(node=None,key=None,timeoutSecs=5,**kwargs):
    if not key: raise Exception('No key for Inspect specified')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.inspect(key, **kwargs)

# since we'll be doing lots of execs on a parsed file, not useful to have parse+exec
# retryDelaySecs isn't used, 
def runExecOnly(node=None,timeoutSecs=20,**kwargs):
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.exec_query(timeoutSecs, **kwargs)

def runGLM(node=None,csvPathname=None,key=None,
        timeoutSecs=20,retryDelaySecs=2,**kwargs):
    parseKey = parseFile(node, csvPathname, key)
    glm = runGLMOnly(node, parseKey, timeoutSecs, retryDelaySecs,**kwargs)
    return glm

def runGLMOnly(node=None,parseKey=None,
        timeoutSecs=20,retryDelaySecs=2,**kwargs):
    if not parseKey: raise Exception('No parsed key for GLM specified')
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.GLM(parseKey['Key'], timeoutSecs, **kwargs)

def runLR(node=None, csvPathname=None,key=None,
        timeoutSecs=20, **kwargs):
    parseKey = parseFile(node, csvPathname, key)
    return runLROnly(node, parseKey, timeoutSecs, **kwargs)

def runLROnly(node=None, parseKey=None, timeoutSecs=20, **kwargs):
    if not parseKey: raise Exception('No parsed key for LR specified')
    if not node: node = h2o.nodes[0]
    # FIX! add something like stabilize in RF to check results, and also retry/timeout
    return node.linear_reg(parseKey['Key'], timeoutSecs, **kwargs)

# there are more RF parameters in **kwargs. see h2o.py
def runRF(node=None, csvPathname=None, trees=5, key=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    parseKey = parseFile(node, csvPathname, key)
    return runRFOnly(node, parseKey, trees, timeoutSecs, retryDelaySecs, **kwargs)

def runRFTreeView(node=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.random_forest_treeview(timeoutSecs, **kwargs)

# there are more RF parameters in **kwargs. see h2o.py
def runRFOnly(node=None, parseKey=None, trees=5,
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for RF specified')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseKey that we should check?
    h2o.verboseprint("runRFOnly parseKey:", parseKey)
    Key = parseKey['Key']
    rf = node.random_forest(Key, timeoutSecs, trees, **kwargs)

    # if we have something in Error, print it!
    # FIX! have to figure out unexpected vs expected errors
    # {u'Error': u'Only integer or enum columns can be classes!'}

    # FIX! right now, the json doesn't always return the same dict keys
    # so have to look to see if Error is present
    if 'Error' in rf:
        if rf['Error'] is not None:
            print "Unexpected Error key/value in rf result:", rf

    # FIX! check all of these somehow?
    dataKey  = rf['dataKey']
    # if we modelKey was given to rf via **kwargs, remove it, since we're passing 
    # modelKey from rf. can't pass it in two places. (ok if it doesn't exist in kwargs)
    kwargs.pop('modelKey',None)
    modelKey = rf['modelKey']

    # same thing. if we use random param generation and have ntree in kwargs, get rid of it.
    kwargs.pop('ntree',None)
    ntree    = rf['ntree']

    # /ip:port of cloud (can't use h2o name)
    rfCloud = rf['h2o']
    # output class?
    rfClass= rf['class']

    def test(n):
        rfView = n.random_forest_view(dataKey, modelKey, timeoutSecs, **kwargs)
        modelSize = rfView['modelSize']
        if (modelSize!=ntree and modelSize>0):
            # don't print the typical case of 0 (starting)
            print "Waiting for RF done: at %d of %d trees" % (modelSize, ntree)
        return modelSize==ntree

    node.stabilize(
            test,
            'random forest reporting %d trees' % ntree,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(dataKey, modelKey, timeoutSecs, **kwargs)
    modelSize = rfView['modelSize']
    confusionKey = rfView['confusionKey']

    # FIX! how am I supposed to verify results, or get results/
    # touch all these just to do something
    cmInspect = node.inspect(confusionKey)
    modelInspect = node.inspect(modelKey)
    dataInspect = node.inspect(dataKey)

    return rfView
