import os, json, unittest, time, shutil, sys
import util.h2o as h2o

class JUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()

    def run_junit(self, javaClass, timeout=None):
        (ps, stdout, stderr) = h2o.spawn_cmd(javaClass, [
                'java', '-ea',
                '-Dh2o.arg.ice_root='+h2o.tmp_dir('ice.'),
                '-jar', h2o.find_file('build/h2o.jar'),
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
        nodes = []
        try:
            nodes = h2o.build_cloud(node_count=2)
            self.run_junit('test.KVTest')
        finally:
            h2o.tear_down_cloud(nodes)

    def testParserTest(self):
        self.run_junit('test.ParserTest')

    def testRFMarginalCasesTest(self):
        nodes = []
        try:
            nodes = h2o.build_cloud(node_count=2)
            self.run_junit('test.DatasetCornerCasesTest')
        finally:
            h2o.tear_down_cloud(nodes)

    def testAppendKeyTest(self):
        nodes = []
        try:
            nodes = h2o.build_cloud(node_count=1)
            self.run_junit('test.AppendKeyTest')
        finally:
            h2o.tear_down_cloud(nodes)

if __name__ == '__main__':
    unittest.main()
