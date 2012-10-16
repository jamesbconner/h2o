import h2o, h2o_cmd
import psutil

h2o.clean_sandbox()
l = None
try:
    l = h2o.LocalH2O(capture_output=False, use_debugger=True)
    print 'Stabilize'
    h2o.stabilize_cloud(l, 1)
    print 'Random Forest'
    h2o_cmd.runRF(l, h2o.find_file('smalldata/poker/poker-hand-testing.data'),
            trees=10, timeoutSecs=60)
    print 'Completed'
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    if l: l.terminate()
