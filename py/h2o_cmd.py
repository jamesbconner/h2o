import os, json, unittest, time, shutil, sys
import h2o
import h2o_browse as h2b

def parseFile(node=None, csvPathname=None, key=None):
    if not csvPathname: raise Exception('No file name specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname, key=key)
    return node.parse(put['key'], put['key']+'.hex')


# don't need X..H2O default is okay (all X), but can pass it as kwargs
def runGLM(node=None,csvPathname=None,Y="1",
    timeoutSecs=30,retryDelaySecs=0.5,
    family="binomial",glm_lambda=None,**kwargs):
    parse = parseFile(node, csvPathname)
    glm = runGLMOnly(node=node, parseKey=parse,Y=Y,
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        family=family,glm_lambda=glm_lambda,**kwargs)
    return glm

# don't need X..H2O default is okay (all X), but can pass it as kwargs
def runGLMOnly(node=None,parseKey=None,Y="1",
    timeoutSecs=30,retryDelaySecs=0.5,
    family="binomial",glm_lambda=None,**kwargs):
    if not parseKey: raise Exception('No file name for GLM specified')
    if not node: node = h2o.nodes[0]
    # FIX! add something like stabilize in RF to check results, and also retry/timeout
    return node.GLM(parseKey['Key'],Y=Y,family=family,glm_lambda=glm_lambda,**kwargs)

# You can change those on the URL line woth "&colA=77&colB=99"
# LinReg draws a line from a collection of points.  Only works if you have 2 or more points.
# will get NaNs if A/B is just one point.

def runLR(node=None, csvPathname=None, **kwargs):
    parse = parseFile(node, csvPathname)
    return runLROnly(node=node, parseKey=parse, **kwargs)

def runLROnly(node=None,parseKey=None,colA=0,colB=1,timeoutSecs=30,retryDelaySecs=0.5):
    if not parseKey: raise Exception('No file name for LR specified')
    if not node: node = h2o.nodes[0]
    # FIX! add something like stabilize in RF to check results, and also retry/timeout
    return node.linear_reg(parseKey['Key'], colA, colB)

###     # we'll have to add something for LR.json to verify the LR results

# there are more RF parameters in **kwargs. see h2o.py
def runRF(node=None, csvPathname=None, **kwargs):
    parse = parseFile(node, csvPathname)
    return runRFOnly(node=node, parseKey=parse, **kwargs)

# there are more RF parameters in **kwargs. see h2o.py
def runRFOnly(node=None, parseKey=None, trees=5, depth=30, 
        timeoutSecs=30, retryDelaySecs=2, browseAlso=False, **kwargs):
    if not parseKey: raise Exception('No parsed key for RF specified')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseKey that we should check?
    h2o.verboseprint("runRFOnly parseKey:",parseKey)
    key = parseKey['Key']
    rf = node.random_forest(key, trees, depth, **kwargs)


    # if we have something in Error, print it!
    # FIX! have to figure out unexpected vs expected errors
    # {u'Error': u'Only integer or enum columns can be classes!'}

    # FIX! right now, the json doesn't always return the same dict keys
    # so have to look to see if Error is present
    if 'Error' in rf:
        if rf['Error'] is not None:
            print "Unexpected Error in rf result:", rf

    # FIX! check all of these somehow?
    dataKey  = rf['dataKey']
    modelKey = rf['modelKey']
    ntree    = rf['ntree']

    # /ip:port of cloud (can't use h2o name)
    rfCloud = rf['h2o']
    # not goal # of trees?, or current that RF is out?. trees is the goal?
    whatIsThis= rf['class']

    # expect response to match the number of trees you asked for
    def test(n):
        # Only passing browse to this guy (and at the end)
        a = n.random_forest_view(dataKey,modelKey,ntree,browseAlso=browseAlso)['modelSize']
        # don't pass back the expected number, for possible fail message
        # so print what we got, if not equal..it's kind of intermediate results that are useful?
        # normally we won't see any?
        # FIX! this is really just for current debug of not-done.
        if (a!=trees and a>0):
            # don't print the typical case of 0 (starting)
            print "Waiting for RF done: at %d of %d trees" % (a, trees)
        return a==trees

    node.stabilize(
            test,
            'random forest reporting %d trees' % trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(dataKey,modelKey,ntree,browseAlso=browseAlso)
    modelSize = rfView['modelSize']
    confusionKey = rfView['confusionKey']

    # FIX! how am I supposed to verify results, or get results/
    # touch all these just to do something
    cmInspect = node.inspect(confusionKey)
    modelInspect = node.inspect(modelKey)
    dataInspect = node.inspect(dataKey)

    return rfView
