import h2o
import test, psutil

babies = []
try:
    print 'Spawn 1'
    babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'), '--port=54321']))
    print 'Spawn 2'
    babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'), '--port=54324']))
    n = h2o.H2O(spawn=False)
    print 'Stabilize'
    h2o.stabilize_cloud(n, 2)
    print 'RF'
    test.runRF(n, 10, h2o.find_file('smalldata/poker/poker-hand-testing.data'), 60)
except KeyboardInterrupt:
    print ''
finally:
    print 'EAT THE BABIES'
    for b in babies: b.kill()
