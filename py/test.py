import os, json, unittest, time
import util.h2o as h2o
import util.asyncproc as proc

def addNode():
    global nodes
    h = h2o.H2O('192.168.1.12', 54321 + len(nodes)*3)
    nodes.append(h)

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global nodes
        try:
            proc.clean_sandbox()
            nodes = []
            addNode()
            addNode()
            addNode()

            # give them a few seconds to stabilize
            nodes[0].stabilize('cloud auto detect', 3,
                lambda n: n.get_cloud()['cloud_size'] == len(nodes))
        except:
            for n in nodes: n.terminate()
            raise

    @classmethod
    def tearDownClass(cls):
        ex = None
        for n in nodes:
            if n.wait() is None:
                n.terminate()
            elif n.wait():
                ex = Exception('Node terminated with non-zero exit code: %d' % n.wait())
        if ex: raise ex

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testBasic(self):
        self.assertEqual(len(nodes), 3)
        for n in nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], 3, 'inconsistent cloud size')

    def testRF(self):
        n = nodes[0]
        put = n.put_file('../smalldata/iris/iris2.csv')
        parse = n.parse(put['keyHref'])
        rf = n.random_forest(parse['keyHref'])
        n.stabilize('random forest finishing', 20,
            lambda n: n.random_forest_view(rf['confKeyHref'])['got'] == 5)


if __name__ == '__main__':
    unittest.main()
