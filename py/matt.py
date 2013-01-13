import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    #h2o.build_cloud(2, java_heap_GB=1, capture_output=False, aws_credentials='/tmp/key.props')
    h2o.nodes = [ h2o.ExternalH2O() ]
    print 'Importing'
    h2o.nodes[0].import_s3('test-s3-integration')
    print 'Parsing'
    h2o.nodes[0].parse('s3:test-s3-integration/covtype.data')
    print 'Done'
    while True:
        time.sleep(0.2)
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
