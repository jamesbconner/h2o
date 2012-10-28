import h2o, h2o_cmd
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    h2o.build_cloud(1, capture_output=False)
    print 'Random Forest'
    h2o_cmd.runRF(None, h2o.find_dataset('UCI/UCI-large/covtype/covtype.data'),
            trees=10, timeoutSecs=60)
    print 'Completed'
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
