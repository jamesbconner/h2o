import unittest, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_badchars(self):
        print "badchars.csv has some 0x0 (<NUL>) characters."
        print "They were created by a dd that filled out to buffer boundary with <NUL>"
        print "They are visible using vim/vi"
        
        csvPathname = h2o.find_file('smalldata/badchars.csv')
        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main()
