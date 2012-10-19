import unittest, os
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_stedo_testing_data(self):
        csvPathname = '../smalldata/stego/stego_testing.data'

        if not os.path.exists(csvPathname):
            raise Exception("Can't find %s." % (csvPathname))

        # Prediction class is the second column => class=1
        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname,clazz=1)

if __name__ == '__main__':
    h2o.unit_main() 
