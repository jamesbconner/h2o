import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import itertools

def file_to_put():
    return 'smalldata/poker/poker1000'

# Dummy wc -l
def wcl(filename):
        lines = 0
        f = open(filename)
        for line in f:
            lines += 1
        
        f.close()
        return lines

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        #pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_A_inspect_poker1000(self):
        cvsfile = h2o.find_file("smalldata/poker/poker1000")
        node    = h2o.nodes[0]
        
        res  = self.putfile_and_parse(node, cvsfile)
        ary  = node.inspect(res['keyHref'])
        # count lines in input file
        rows = wcl(cvsfile)

        self.assertEqual(rows, ary['rows'])
        self.assertEqual(11, ary['cols'])

    def putfile_and_parse(self, node, f):
        result  = node.put_file(f)
        key     = result['keyHref']
        return node.parse(key);

    
if __name__ == '__main__':
    h2o.unit_main()
