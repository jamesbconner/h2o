import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd
import itertools

def file_to_put():
    return 'smalldata/poker/poker1000'

def crange(start, end):
    for c in xrange(ord(start), ord(end)):
        yield chr(c)

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

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_A_inspect_poker1000(self):
        cvsfile = h2o.find_file("smalldata/poker/poker1000")
        node    = h2o.nodes[0]
        
        res  = self.putfile_and_parse(node, cvsfile)
        ary  = node.inspect(res['keyHref'])
        # count lines in input file - there is no header for poker 1000
        rows = wcl(cvsfile)

        self.assertEqual(rows, ary['rows'])
        self.assertEqual(11, ary['cols'])

    def test_B_inspect_column_names_multi_space_sep(self):
        self.inspect_column_names("smalldata/test/test_26cols_multi_space_sep.csv")

    def test_C_inspect_column_names_single_space_sep(self):
        self.inspect_column_names("smalldata/test/test_26cols_single_space_sep.csv")

    def test_D_inspect_column_names_comma_sep(self):
        self.inspect_column_names("smalldata/test/test_26cols_comma_sep.csv")

    def test_E_inspect_column_names_comma_sep(self):
        self.inspect_column_names("smalldata/test/test_26cols_single_space_sep_2.csv")


    # Shared test implementation for smalldata/test/test_26cols_*.csv
    def inspect_column_names(self, filename):
        cvsfile = h2o.find_file(filename)
        node    = h2o.nodes[0]
        
        res  = self.putfile_and_parse(node, cvsfile)
        ary  = node.inspect(res['keyHref'])

        self.assertEqual(1, ary['rows'])
        self.assertEqual(26, ary['cols'])

        for (col, expName) in zip(ary['columns'], crange('A', 'Z')):
            #h2o.verboseprint(expName, col['name'])            
            self.assertEqual(expName, col['name'])

    def putfile_and_parse(self, node, f):
        result  = node.put_file(f)
        key     = result['keyHref']
        return node.parse(key);

    
if __name__ == '__main__':
    h2o.unit_main()
