import os, json, unittest, time, shutil, sys
import util.h2o as h2o

class JUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        global nodes
        nodes = h2o.build_cloud(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(nodes)

    def run_junit(self, javaClass, timeout=None):
        (ps, stdout, stderr) = h2o.spawn_cmd(javaClass, [
                'java', '-jar', '../build/h2o.jar',
                '-mainClass', 'org.junit.runner.JUnitCore',
                javaClass
        ])
        rc = ps.wait(timeout)
        out = file(stdout).read()
        err = file(stderr).read()
        if rc is None:
            rc.terminate()
            raise Exception("%s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                    (javaClass, timeout or 0, out, err))
        elif rc != 0:
            raise Exception("%s failed.\nstdout:\n%s\n\nstderr:\n%s" % (javaClass, out, err))

    def testKVTest(self):
        self.run_junit('test.KVTest')

    def testParserTest(self):
        self.run_junit('test.ParserTest')

if __name__ == '__main__':
    unittest.main()
