import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    #h2o.build_cloud(1,java_heap_GB=1,capture_output=False)
    h2o.nodes = [ h2o.ExternalH2O() ]
    print 'Parsing'
    h2o_cmd.parseFile(csvPathname=h2o.find_file('smalldata/fail1_100x11000.csv.gz'))
    print 'Done'
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    #h2o.tear_down_cloud()
