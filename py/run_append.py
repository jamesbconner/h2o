import h2o
import test, psutil

babies = []
try:
    print 'Spawn'
    babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'), '--port=54324']))
    babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'), '--port=54327']))
    babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'),
                '-mainClass', 'org.junit.runner.JUnitCore', 'test.KVTest']))
    n = h2o.H2O(spawn=False)
    print 'Stabilize'
    h2o.stabilize_cloud(n, 3)
    babies[2].wait(60)
except KeyboardInterrupt:
    print ''
finally:
    print 'EAT THE BABIES'
    for b in babies: b.kill()
