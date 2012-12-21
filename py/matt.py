import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    h2o.build_cloud(3, java_heap_GB=4, capture_output=False)
    n = h2o.nodes[0]
    print 'Import'
    n.import_folder('/Users/fowles/dev/datasets')
    print 'Parse'
    k = 'nfs://Users/fowles/dev/datasets/allstate.csv'
    n.parse(k)
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
