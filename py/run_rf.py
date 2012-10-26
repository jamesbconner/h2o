import os, json, unittest, time, shutil, sys
import h2o_cmd, h2o

try:
    print 'Connecting to hosts'
    hosts = [
        h2o.RemoteHost('rufus.local','fowles'),
        h2o.RemoteHost('eiji.local', 'boots'),
    ]
    print 'Building cloud'
    h2o.build_cloud(node_count=3, hosts=hosts)

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

    print 'Running trials'
    for trials in xrange(1,10000):
        # prime
        trees = 6
        y = 10000 * 161

        csvFilename = "parity_128_4_" + str(y) + "_quad.data"  
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        # FIX! TBD do we always have to kick off the run from node 0?
        # random guess about length of time, varying with more hosts/nodes?
        timeoutSecs = 20 + 5*(len(h2o.nodes))
        # for debug (browser)
        ###timeoutSecs = 3600
        # RFview consumes cycles. Only retry once a second, to avoid slowing things down
        # this also does the put, which is probably a good thing to try multiple times also

        # change the model name each iteration, so they stay in h2o
        modelKey = csvFilename + "_" + str(trials)
        h2o_cmd.runRF(trees=trees, modelKey=modelKey, timeoutSecs=timeoutSecs, 
            retryDelaySecs=1, csvPathname=csvPathname)
        sys.stdout.write('.')
        sys.stdout.flush()
except KeyboardInterrupt:
    print 'Interrupted'
except Exception, e:
    print 'Exception', e
    while True:
        time.sleep(1)
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
