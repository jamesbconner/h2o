import os, json, unittest, time, shutil, sys
import h2o

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
    node.stabilize(
            lambda n: 
                (n.random_forest_view(rf['confKeyHref'])['got']==trees) or
                (n.random_forest_view(rf['confKeyHref'])['got']==len(h2o.nodes)*trees),
            'random forest reporting %d trees' % trees,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
