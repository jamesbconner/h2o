import os, json, unittest, time, shutil, sys, time
import h2o_cmd, h2o

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global hosts
        global nodes
        global nodes_per_host

        # we may have big delays with 2 jvms (os?)
        # also: what is the agreement on json visible cloud state in each node, vs the paxos algorithm
        # (timing)
        nodes_per_host = 1
        hosts = []
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
        else: # local configuration for michal, change the condition 1==0 above to test in 0xdata intranet
            hosts = [
                h2o.RemoteHost('195.113.21.151', 'malom1am'),
                h2o.RemoteHost('195.113.21.152', 'malom1am'),
                h2o.RemoteHost('195.113.21.153', 'malom1am'),
                h2o.RemoteHost('195.113.21.154', 'malom1am'),
            ]

        h2o.upload_jar_to_remote_hosts(hosts)
        # build cloud
        nodes = h2o.build_remote_cloud(hosts, nodes_per_host, 
                base_port=55321, ports_per_node=3, sigar=True)


    @classmethod
    def tearDownClass(cls):
        h2o.verboseprint("Tearing down cloud")
        h2o.tear_down_cloud(nodes)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sigar_remote(self):
        # Nasty trick - let remote virtual machines to exchange several heartbeat packets to obtain network statistics
        h2o.verboseprint("Waiting...")
        time.sleep(10)
        
        # Access each node 
        for n in nodes:
            # Access Sigar API (originally it throws an JNI-based exception)
            netstats = n.netstat()
            # Check the cloud if it is alive (if not the exception was thrown)
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')
            h2o.verboseprint("Checking:", n)
            h2o.verboseprint("Cloud: ", c)
            h2o.verboseprint("Netstat ", netstats)



if __name__ == '__main__':
    h2o.unit_main()

