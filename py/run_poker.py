import h2o, cmd
import psutil

h2o.clean_sandbox()
babies = []
try:
    for i in xrange(8):
        babies.append(psutil.Popen(['java', '-ea', '-jar', h2o.find_file('build/h2o.jar'), '--port=%d' % (54321+3*i)]))
    n = h2o.H2O(spawn=False)
    h2o.nodes += [n] * len(babies)
    print 'Stabilize'
    h2o.stabilize_cloud(n, len(babies))
    print 'Random Forest'
    cmd.runRF(n, h2o.find_file('smalldata/poker/poker-hand-testing.data'),
            trees=10, timeoutSecs=60)
    print 'Completed'
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    for b in babies: b.kill()
