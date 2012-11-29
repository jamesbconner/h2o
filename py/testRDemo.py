import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd as cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_R_Demo(self):
        rCmdString = "R -f ../R/H2OTestDemo.R"
        h2o.spawn_cmd_and_wait('rdemo', rCmdString.split())

if __name__ == '__main__':
    h2o.unit_main()
