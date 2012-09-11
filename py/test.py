import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc

class Basic(unittest.TestCase):
    def addNode(self):
        h = h2o.H2O(54321 + len(self.nodes)*3)
        self.nodes.append(h)

    def setUp(self):
        self.nodes = []
        proc.clean_sandbox()
        self.addNode()
        self.addNode()
        self.addNode()

        # give them a few seconds to stabilize
        self.nodes[0].stabilize('cloud auto detect', 3,
            lambda n: n.get_cloud()['cloud_size'] == len(self.nodes))

    def tearDown(self):
        for n in self.nodes:
            n.terminate()

    def testBasic(self):
        self.assertEqual(len(self.nodes), 3)
        for n in self.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], 3, 'inconsistent cloud size')

    def testRF(self):
        n = self.nodes[0]
        put = n.put_file('../smalldata/iris/iris2.csv')
        parse = n.parse(put['key'])
        rf = n.random_forest(parse['Key'])
        rfv = {}
        rfv['Key'] = rf['treeskey']
        rfv['origKey'] = rf['origKey']
        n.stabilize('random forest finishing', 20,
            lambda n: n.random_forest_view(rfv)['got'] == 5)


if __name__ == '__main__':
    unittest.main()
