import h2o, h2o_cmd
import psutil

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    h2o.build_cloud(2, capture_output=False)
    print 'Random Forest'
    h2o_cmd.runRF(None, h2o.find_file('smalldata/poker/poker-hand-testing.data'),
            trees=10, timeoutSecs=60)
    print 'Completed'
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
