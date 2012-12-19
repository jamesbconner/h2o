import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_browse as h2b


def write_syn_dataset(csvPathname, rowCount, headerData, rowData):
    dsf = open(csvPathname, "w+")
    
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        dsf.write(rowData + "\n")
    dsf.close()

# append!
def append_syn_dataset(csvPathname, rowData):
    with open(csvPathname, "a") as dsf:
        dsf.write(rowData + "\n")

class glm_same_parse(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # fails with 3
        h2o.build_cloud(3)
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        if not h2o.browse_disable:
            time.sleep(500000)

        h2o.tear_down_cloud(h2o.nodes)
    
    def test_sort_of_prostate_with_row_schmoo(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        key2 = csvFilename + ".hex"

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        rowData = "1,0,65,1,2,1,1.4,0,6"

        write_syn_dataset(csvPathname,      99860, headerData, rowData)

        print "This is the same format/data file used by test_same_parse, but the non-gzed version"
        print "\nSchmoo the # of rows"
        for trial in range (200):
            append_syn_dataset(csvPathname, rowData)
            ### start = time.time()
            ### key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate.csv"))
            ### print "Trial #", trial, "parse end on ", "prostate.csv" , 'took', time.time() - start, 'seconds'

            start = time.time()
            key = h2o_cmd.parseFile(csvPathname=csvPathname, browseAlso=True, key=csvFilename, key2=csvFilename+".hex")
            print "trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'

            h2o_cmd.runInspect(key=key2)
            h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            
            if (h2o.check_sandbox_for_errors()):
                raise Exception("Found errors in sandbox stdout or stderr, on trial #%s." % trial)

if __name__ == '__main__':
    h2o.unit_main()
