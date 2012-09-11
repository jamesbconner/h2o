import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc

class Basic(unittest.TestCase):
    def addNode(self):
        h = h2o.H2O('192.168.1.17', 54321 + len(self.nodes)*3)
        self.nodes.append(h)

    def setUp(self):
        try:
            self.nodes = []
            proc.clean_sandbox()
            self.addNode()
            self.addNode()
            self.addNode()

            # give them a few seconds to stabilize
            self.nodes[0].stabilize('cloud auto detect', 3,
                lambda n: n.get_cloud()['cloud_size'] == len(self.nodes))
        except:
            for n in self.nodes: n.terminate()
            raise

    def tearDown(self):
        ex = None
        for n in self.nodes:
            if n.wait() is None:
                n.terminate()
            elif n.wait():
                ex = Exception('Node terminated with non-zero exit code: %d' % n.wait())
        if ex: raise ex

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
        n.stabilize('random forest finishing', 20,
            lambda n: n.random_forest_view(rf['confKey'])['got'] == 5)


if __name__ == '__main__':
    unittest.main()
