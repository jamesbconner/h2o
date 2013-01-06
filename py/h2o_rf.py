# currently:
# random_forest_view result: {
#   "class": 8, 
#   "confusion_key": "ConfusionMatrix of (1dd49500-cb8b-415a-a7f2-4bb8f4f716ce.hex[8],pytest_model[237],0)", 
#   "confusion_matrix": {
#     "header": [ "0", "1", "2", "3" ], 
#     "scores": [
#       [ 1250, 0, 246, 1004 ], 
#       [ 0, 1875, 113, 512 ], 
#       [ 0, 0, 1483, 1017 ], 
#       [ 0, 0, 232, 2268 ]
#     ], 
#     "type": "full scoring", 
#     "used_trees": 237
#   }, 
#   "data_key": "1dd49500-cb8b-415a-a7f2-4bb8f4f716ce.hex", 
#   "model_key": "pytest_model", 
#   "ntree": 237, 
#   "response": {
#     "h2o": "pytest-kevin-28041", 
#     "node": "/192.168.0.37:54321", 
#     "status": "done", 
#     "time": 1
#   }, 
#   "trees": {
#     "depth": " 7.0 / 11.7 / 15.0", 
#     "leaves": " 8.0 / 16.2 / 31.0", 
#     "number_built": 237
#   }

import h2o

def simpleCheckRFView(node, rfv,**kwargs):
    if not node:
        node = h2o.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    oclass = rfv['class']
    if (oclass<0 or oclass>20000):
        raise Exception("class in RFView seems wrong. class:", oclass)

    # the simple assigns will at least check the key exists
    cm = rfv['confusion_matrix']
    header = cm['header'] # list
    scores = cm['scores'][0] # list
    if (sum(scores) < 1):
        raise Exception("scores in RFView seems wrong. scores:", oscores)

    type = cm['type']
    used_trees = cm['used_trees']
    if (used_trees<=0 or used_trees>20000):
        raise Exception("used_trees in RFView seems wrong. used_trees:", used_trees)

    data_key = rfv['data_key']
    model_key = rfv['model_key']
    ntree = rfv['ntree']
    if (ntree<=0 or ntree>20000):
        raise Exception("ntree in RFView seems wrong. ntree:", ntree)
    response = rfv['response'] # Dict
    h2o = response['h2o']
    rfv_node = response['node']
    status = response['status']
    time = response['time']

    trees = rfv['trees'] # Dict
    depth = trees['depth']
    if ' 0.0 ' in depth:
        raise Exception("depth in RFView seems wrong. depth:", depth)
    leaves = trees['leaves']
    if ' 0.0 ' in leaves:
        raise Exception("leaves in RFView seems wrong. leaves:", leaves)
    number_built = trees['number_built']
    if (number_built<=0 or number_built>20000):
        raise Exception("number_built in RFView seems wrong. number_built:", number_built)


    print "RFView response: number_built:", number_built, "leaves:", leaves, "depth:", depth

    # just touching these keys to make sure they're good?
    confusion_key = rfv['confusion_key']
    model_key = rfv['model_key']
    data_key = rfv['data_key']

    confusionInspect = node.inspect(confusion_key)
    modelInspect = node.inspect(model_key)
    dataInspect = node.inspect(data_key)



