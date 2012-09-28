import os, json, unittest, time, shutil, sys
import h2o

class JUnit(unittest.TestCase):
    def run_junit(self, javaClass, timeout=None):
        # we don't have the port or ip configuration here 
        # that util/h2o.py does? Keep this in synch with spawn_h2o there.
        # also don't have --nosigar here?
        (ps, stdout, stderr) = h2o.spawn_cmd(javaClass, [
                'java', 
                '-Dh2o.arg.ice_root='+h2o.tmp_dir('ice.'),
                '-javaagent:' + h2o.find_file('build/h2o.jar'),
                '-ea', '-jar', h2o.find_file('build/h2o.jar'),
                '-mainClass', 'org.junit.runner.JUnitCore',
                javaClass
        ])

        rc = ps.wait(timeout)
        out = file(stdout).read()
        err = file(stderr).read()
        if rc is None:
            ps.terminate()
            raise Exception("%s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                    (javaClass, timeout or 0, out, err))
        elif rc != 0:
            raise Exception("%s failed.\nstdout:\n%s\n\nstderr:\n%s" % (javaClass, out, err))

    def testKVTest(self):
        try:
            h2o.build_cloud(node_count=2)
            self.run_junit('test.KVTest')
        finally:
            h2o.tear_down_cloud()

    def testParserTest(self):
        self.run_junit('test.ParserTest')

    def testRTSerGenHelperTest(self):
        self.run_junit('test.RTSerGenHelperTest')

    def testRFMarginalCasesTest(self):
        try:
            h2o.build_cloud(node_count=2)
            self.run_junit('test.DatasetCornerCasesTest')
        finally:
            h2o.tear_down_cloud()

    def testAppendKeyTest(self):
        try:
            h2o.build_cloud(node_count=1)
            self.run_junit('test.AppendKeyTest')
        finally:
            h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.clean_sandbox()
    unittest.main()
