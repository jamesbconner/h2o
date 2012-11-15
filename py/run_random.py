import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    h2o.build_cloud(1, capture_output=False)
    print 'Random Forest'
    h2o_cmd.runRF(None, h2o.find_file('smalldata/iris/iris2.csv'))
    print 'Completed'
    h2b.browseJsonHistoryAsUrlLastMatch("RFView")
    h2o_cmd.parseFile(None, h2o.find_file('smalldata/iris/iris.csv'), 'iris')
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
