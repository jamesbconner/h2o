import os, json, unittest, time, shutil, sys, time
import h2o_cmd, h2o, itertools

def file_to_put():
    return 'smalldata/poker/poker-hand-testing.data'

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global nodes
        nodes_per_host = 1
        hosts = []

        h2o.verboseprint("About to RemoteHost, likely bad ip if hangs")
        if (1==0):
            hosts = [
                h2o.RemoteHost('192.168.1.150', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.151', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.152', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.153', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.154', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.155', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.156', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.160', '0xdiag', '0xdiag'),
            ]
        elif (1==0): # local configuration for michal, change the condition 1==0 above to test in 0xdata intranet
            hosts = [
                h2o.RemoteHost('195.113.21.151', 'malom1am'),
                h2o.RemoteHost('195.113.21.152', 'malom1am'),
                h2o.RemoteHost('195.113.21.153', 'malom1am'),
                h2o.RemoteHost('195.113.21.154', 'malom1am'),
            ]
        else: 
            #    h2o.RemoteHost('192.168.0.35', '0xdiag','0xdiag'),
            hosts = [
                h2o.RemoteHost('192.168.0.37', '0xdiag','0xdiag')
            ]


        # h2o.upload_jar_to_remote_hosts(hosts)
        # build cloud
        # sigar=True
        nodes = h2o.build_cloud(nodes_per_host, 
                base_port=55321, ports_per_node=3, hosts=hosts)


    @classmethod
    def tearDownClass(cls):
        h2o.verboseprint("Tearing down cloud")
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # Try to put a file to each node in the cloud and checked reported size of the saved file 
    def test_A_putfile_to_all_nodes(self):
        
        cvsfile  = h2o.find_file(file_to_put())
        origSize = h2o.get_file_size(cvsfile)

        # Putfile to each node and check the returned size
        for node in nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file:", cvsfile, "node:", node, "origSize:", origSize)
            result     = node.put_file(cvsfile)
            returnSize = result['size']
            self.assertEqual(origSize,returnSize)

    # Try to put a file, get file and diff orinal file and returned file.
    def test_B_putfile_and_getfile_to_all_nodes(self):

        cvsfile = h2o.find_file(file_to_put())
        for node in h2o.nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file", cvsfile, "to", node)
            result = node.put_file(cvsfile)
            key    = result['keyHref']
            r      = node.get_key(key)
            f      = open(cvsfile)
            self.diff(r, f)
            f.close()

    def diff(self,r, f):
        h2o.verboseprint("checking r and f:", r, f)
        for (r_chunk,f_chunk) in itertools.izip(r.iter_content(1024), h2o.iter_chunked_file(f, 1024)):
            self.assertEqual(r_chunk,f_chunk)


if __name__ == '__main__':
    h2o.unit_main()
    print "hello2"

