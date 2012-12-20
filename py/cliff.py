import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

try:
    h2o.build_cloud(2,capture_output=False,classpath=True,username="Cliff Click")
    #for trial in range (10):
    #    start = time.time()
    #    key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate_long.csv"))
    #    print "trial #", trial, "parse end on ", "prostate_long.csv" , 'took', time.time() - start, 'seconds'
    while True: time.sleep(1)
finally:
    print "hello"
    h2o.tear_down_cloud(h2o.nodes)
    
