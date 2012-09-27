import os, json, unittest, time, shutil, sys
import h2o

class StartUp(unittest.TestCase):
    def test_concurrent_startup(self):
        babies = []
        num_babies = 3
        try:
            for i in xrange(num_babies):
                babies.append(h2o.spawn_h2o(port=54321+3*i,nosigar=False))
            n = h2o.H2O(port=54321, spawn=False)
            h2o.stabilize_cloud(n, num_babies)
# TODO: access sigar API through json request
        except:
            err = 'Error starting %d nodes quickly' % num_babies
            for b in babies:
                err += '\nVM %d stdout:\n%s\nstderr%s\n' % (
                    b[0].pid, file(b[1]).read(), file(b[2]).read())
            raise Exception(err)
        finally:
            # EAT THE BABIES!
            for b in babies: b[0].terminate()


if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
