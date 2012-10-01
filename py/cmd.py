import os, json, unittest, time, shutil, sys
import h2o

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
    # lr = node.linear_reg(parseKey['keyHref'], colA, colB)
    node.linear_reg(parseKey['keyHref'], colA, colB)

###     # we'll have to add something for LR.json to verify the LR results
###     node.stabilize(
###             lambda n: 
###                 (n.linear_reg_view(lr['confKeyHref'])['got']==trees) or
###                 (n.linear_reg_view(lr['confKeyHref'])['got']==len(h2o.nodes)*trees),
###             'linear regression reporting %d trees' % trees,
###             timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

def runRF(node=None, csvPathname=None, trees=5, timeoutSecs=30, retryDelaySecs=2):
    if not csvPathname: raise Exception('No file name for RF specified')
    if not node: node = h2o.nodes[0]
    put = node.put_file(csvPathname)
    parse = node.parse(put['keyHref'])
    runRFOnly(node=node, parseKey=parse, trees=trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

def runRFOnly(node=None, parseKey=None, trees=5, depth=30,
        timeoutSecs=30, retryDelaySecs=2):
    if not parseKey: raise Exception('No file name for RF specified')
    if not node: node = h2o.nodes[0]
    rf = node.random_forest(parseKey['keyHref'], trees, depth)
    # this expects the response to match the number of trees you told it to do
    # FIX! temporary hack to allow nodes*trees to be a legal final response also
    node.stabilize(
            lambda n: 
                (n.random_forest_view(rf['confKeyHref'])['got']==trees) or
                (n.random_forest_view(rf['confKeyHref'])['got']==len(h2o.nodes)*trees),
            'random forest reporting %d trees' % trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
