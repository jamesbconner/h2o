import os, json, unittest, time, shutil, sys, time
import h2o_cmd, h2o
import time

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global hosts
        global nodes
        global nodes_per_host

        # FIX! doesn't clean up old stuff in /tmp. Can overflow. Clean up with ssh commands outside this
        # every once in a while

        # ssh limited to 10 somehow? did /etc/ssh/sshd_config: MaxSessions 20, MaxAuth 20
        # maybe sftp?
        nodes_per_host = 10
        # we may have big delays with 2 jvms (os?)
        # also: what is the agreement on json visible cloud state in each node vs the paxos algorithm
        hosts = []
        # FIX! probably will just add args for -matt -kevin -0xdata that select different lists?
        # or we could have a hosts file that's local that you modify. just as easy to mod this though?
        if (1==0):

            # 0xdiag user. ubuntu okay with: sudo adduser --force-badname 0xdiag
            #    h2o.RemoteHost('192.168.0.37',  '0xdiag', '0xdiag')
            hosts = [
                h2o.RemoteHost('192.168.1.17', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.150', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.151', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.152', '0xdiag', '0xdiag'),
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
                # no 157
                h2o.RemoteHost('192.168.1.160', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.161', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.17', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.20', '0xdiag', '0xdiag'),
            ]
        elif (1==0) :
            hosts = [
                h2o.RemoteHost('192.168.0.33', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.0.35', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.0.37', '0xdiag', '0xdiag'),
            ]
        else:
            hosts = [
                h2o.RemoteHost('192.168.1.17', '0xdiag', '0xdiag'),
                h2o.RemoteHost('192.168.1.20', '0xdiag', '0xdiag'),
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
        trial = 0
        for i in range(1,100):
            sys.stdout.write('.')
            sys.stdout.flush()

            # nodes_per_host is per host.
            nodes = h2o.build_cloud(nodes_per_host, base_port=56321, ports_per_node=3, hosts=hosts,
                timeoutSecs=60, retryDelaySecs=1)

            # FIX! if node[0] is fast, maybe the other nodes aren't at a point where they won't get
            # connection errors. Stabilize them too! Can have short timeout here, because they should be 
            # stable?? or close??

            # FIX! using "consensus" in node[0] should mean this is unnecessary?
            # maybe there's a bug
            for n in nodes:
                h2o.stabilize_cloud(n, len(nodes), timeoutSecs=30, retryDelaySecs=1)

            # now double check ...no stabilize tolerance of connection errors here
            for n in nodes:
                print "Checking n:", n
                c = n.get_cloud()
                self.assertFalse(c['cloud_size'] > len(nodes), 'More nodes than we want. Zombie JVMs to kill?')
                self.assertEqual(c['cloud_size'], len(nodes), 'inconsistent cloud size')

            trial += 1
            # FIX! is this really needed? does tear_down_cloud do shutdown command and wait?
            print "Sending shutdown to cloud, trial", trial
            h2o.nodes[0].shutdown_all()
            time.sleep(1)

            print "Tearing down cloud, trial", trial
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.tear_down_cloud(nodes)
            h2o.clean_sandbox()
            # wait to make sure no sticky ports or anything os-related
            time.sleep(4)


if __name__ == '__main__':
    h2o.unit_main()

