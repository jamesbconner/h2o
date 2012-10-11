import os, json, unittest, time, shutil, sys, time
import h2o_cmd, h2o

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global hosts
        global nodes
        global node_count

        # we may have big delays with 2 jvms (os?)
        # also: what is the agreement on json visible cloud state in each node vs the paxos algorithm
        node_count = 1
        hosts = []
        # FIX! probably will just add args for -matt -kevin -0xdata that select different lists?
        # or we could have a hosts file that's local that you modify. just as easy to mod this though?
        if (1==0):

            # ubuntu okay with: sudo adduser --force-badname 0xdiag
            #    h2o.RemoteHost('192.168.0.37',  '0xdiag', '0xdiag')
            hosts = [
                h2o.RemoteHost('192.168.1.17', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.150', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.151', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.152', '0xdiag', '0xdiag')
            ]
        elif (1==1):

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
        else:
            hosts = [
                h2o.RemoteHost('192.168.0.35', 'diag0x', 'diag0x'),
                h2o.RemoteHost('192.168.0.37', 'diag0x', 'diag0x')
            ]

        h2o.upload_jar_to_remote_hosts(hosts)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_remote_Cloud(self):
        # FIX! too mfiles open at 240 iterations. file closes?
        trial = 0
        for i in range(1,100):
            sys.stdout.write('.')
            sys.stdout.flush()

            # node_count is per host.
            nodes = h2o.build_cloud(node_count, base_port=56321, ports_per_node=3, hosts=hosts)

            # FIX! if node[0] is fast, maybe the other nodes aren't at a point where they won't get
            # connection errors. Stabilize them too! Can have short timeout here, because they should be 
            # stable?? or close??
            for n in nodes:
                h2o.stabilize_cloud(n, len(nodes), timeoutSecs=3, retryDelaySecs=0.25)

            # now double check ...no stabilize tolerance of connection errors here
            for n in nodes:
                print "Checking n:", n
                c = n.get_cloud()
                self.assertFalse(c['cloud_size'] > len(nodes), 'More nodes than we want. Zombie JVMs to kill?')
                self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

            trial += 1
            print "Tearing down cloud, trial", trial
            h2o.tear_down_cloud(nodes)
            h2o.clean_sandbox()
            # kbn. temp hack ..wait for sticky open ports to close? (since we reuse the ports)
            # time.sleep(4)


if __name__ == '__main__':
    h2o.unit_main()

