import h2o, h2o_cmd
# assume the importFolder prefix is datasets, for now
# just do the import folder once
def setupImportFolder(node=None, importFolderPath='/home/0xdiag/datasets'):
    if not node: node = h2o.nodes[0]
    importFolderResult = node.import_folder(importFolderPath)
    h2o.dump_json(importFolderResult)
    return importFolderResult

# assumes you call setupImportFolder first
def parseImportFolderFile(node=None, csvFilename=None, importFolderPath=None, key2=None,
    timeoutSecs=None, retryDelaySecs=None):
    if not node: node = h2o.nodes[0]
    if not csvFilename: raise Exception('parseImportFolderFile: No csvFilename')

    csvPathnameForH2O = "nfs:/" + importFolderPath + "/" + csvFilename

    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = csvFilename + '.hex'
    else:
        myKey2 = key2

    # We like the short parse key2 name. 
    # We don't drop anything from csvFilename, unlike H2O default
    print "Waiting for the slow parse of the file:", csvFilename
    parseKey = node.parse(csvPathnameForH2O, myKey2, 
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
    print "\nParse result:", parseKey

    return parseKey

# FIX! can update this to parse from local dir also (import keys from folder?)
# but everyone needs to have a copy then
def parseHdfsFile(node=None, csvFilename=None, timeoutSecs=3600, retryDelaySecs=1.0):
    if not csvFilename: raise Exception('No csvFilename parameter in inspectHdfsFile')
    if not node: node = h2o.nodes[0]

    # assume the hdfs prefix is datasets, for now
    print "\nHacked the test to match the new behavior for key names created from hdfs files"
    print "Was: hdfs:// prefix"
    print "Was: hdfs://192.168.1.151/datasets/ prefix"
    print "Now: hdfs:// prefix"
    
    # FIX! this is ugly..needs to change to use the name node from the config json/h2o args?
    # also the hdfs dir
    hdfsPrefix = "hdfs://192.168.1.151/datasets/"
    hdfsPrefix = "hdfs://"
    # temp hack to match current H2O
    hdfsPrefix = "hdfs:/datasets/"
    hdfsKey = hdfsPrefix + csvFilename
    print "parseHdfsFile hdfsKey:", hdfsKey

    # FIX! getting H2O HPE?
    # inspect = node.inspect(hdfsKey)
    inspect = h2o_cmd.runInspect(key=hdfsKey)
    print "parseHdfsFile:", inspect

    parseKey = node.parse(key=hdfsKey, key2=csvFilename + ".hex", 
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
    print "parseHdfsFile:", parseKey
    return parseKey
