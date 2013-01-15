import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

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

try:
    print 'Building cloud'
    #h2o.build_cloud(1,java_heap_GB=1,capture_output=False)
    h2o.nodes = [h2o.ExternalH2O()]

    print 'Generating'
    SYNDATASETS_DIR = h2o.make_syn_dir()
    csvFilename = "syn_prostate.csv"
    csvPathname = SYNDATASETS_DIR + '/' + csvFilename
    headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
    rowData = "1,0,65,1,2,1,1.4,0,6"
    write_syn_dataset(csvPathname,      99860, headerData, rowData)

    append_syn_dataset(csvPathname, rowData)
    append_syn_dataset(csvPathname, rowData)
    start = time.time()
    key = csvFilename + "_" + str(0)
    key2 = csvFilename + "_" + str(0) + ".hex"
    print 'Parsing'
    key = h2o_cmd.parseFile(csvPathname=csvPathname, key=key, key2=key2)
    print 'Done'
    while True:
        time.sleep(0.2)
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
