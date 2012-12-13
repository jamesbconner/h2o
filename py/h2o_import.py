import h2o
# assume the importFolder prefix is datasets, for now
# just do the import folder once
def setupImportFolder(node=None, importFolderPath='/home/0xdiag/datasets'):
    if not node: node = h2o.nodes[0]
    importFolderResult = node.import_folder(importFolderPath)
    h2o.dump_json(importFolderResult)

# assumes you call setupImportFolder first
def parseImportFolderFile(node=None, csvFilename=None, importFolderPath=None, key2=None,
    timeoutSecs=None):
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
    parseKey = node.parse(csvPathnameForH2O, myKey2, timeoutSecs)
    print "\nParse result:", parseKey

    return parseKey
