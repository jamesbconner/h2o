import os, json, unittest, time, shutil, sys
import util.h2o as h2o

def quick_startup(num_babies=3, nosigar=True):
    babies = []
    try:
        for i in xrange(num_babies):
            babies.append(h2o.spawn_h2o(port=54321+3*i,nosigar=nosigar))
        n = h2o.H2O(port=54321, spawn=False)
        h2o.stabilize_cloud(n, num_babies)
        print n.netstat()
    except:
        err = 'Error starting %d nodes quickly (sigar is %s)' % (num_babies,'disabled' if nosigar else 'enabled')
        for b in babies:
            err += '\nVM %d stdout:\n%s\nstderr%s\n' % (
                b[0].pid, file(b[1]).read(), file(b[2]).read())
        raise Exception(err)
    finally:
        # EAT THE BABIES!
        kids = [b[0] for b in babies]
        print kids
        #h2o.tear_down_cloud(kids)
        for b in babies: b[0].terminate()

class StartUp(unittest.TestCase):
    def test_concurrent_startup(self):
        quick_startup(num_babies=3)

    # NOTE: we do not need to access Sigar API, since launched nodes
    # start sending heartbeat packets which contain information taken 
    # from Sigar
    def test_concurrent_startup_with_sigar(self):
        #time.sleep(1)
        quick_startup(num_babies=3,nosigar=False)

if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
