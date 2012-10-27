import os, json, unittest, time, shutil, sys
import h2o_cmd, h2o
import h2o_browse as h2b

try:
    h2o.clean_sandbox()
    print 'Building cloud'
    h2o.build_cloud(node_count=6)

    SYNDATASETS_DIR = './syn_datasets'
    if os.path.exists(SYNDATASETS_DIR):
        shutil.rmtree(SYNDATASETS_DIR)
    os.mkdir(SYNDATASETS_DIR)

    SYNSCRIPTS_DIR = './syn_scripts'

    # always match the run below!
    print 'Generating data sets'
    # more rows!
    y = 10000 * 161
    # Have to split the string out to list for pipe
    shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(y) + " quad"
    # FIX! as long as we're doing a couple, you'd think we wouldn't have to 
    # wait for the last one to be gen'ed here before we start the first below.
    # large row counts. need more time
    h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=90)
    # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
    csvFilename = "parity_128_4_161_quad.data"  

    print 'Put and parse'
    # prime
    trees = 6
    y = 10000 * 161
    csvFilename = "parity_128_4_" + str(y) + "_quad.data"  
    csvPathname = SYNDATASETS_DIR + '/' + csvFilename

    reparse = True
    if not reparse:
        node = h2o.nodes[0]
        put = node.put_file(csvPathname)
        parse = node.parse(put['key'])

    print 'Running trials'
    for trials in xrange(1,10000):
        if reparse:
            node = h2o.nodes[0]
            put = node.put_file(csvPathname)
            parse = node.parse(put['key'])

        timeoutSecs = 20 + 5*(len(h2o.nodes))
        modelKey = csvFilename + "_" + str(trials)
        h2o_cmd.runRFOnly(parseKey=parse, trees=trees,
                modelKey=modelKey, timeoutSecs=timeoutSecs, retryDelaySecs=2)
        sys.stdout.write('.')
        sys.stdout.flush()
except KeyboardInterrupt:
    print 'Interrupted'
except Exception, e:
    print 'Exception', e
    h2b.browseJsonHistoryAsUrlLastMatch("RFView")
    while True:
        time.sleep(1)
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
