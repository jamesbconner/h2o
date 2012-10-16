import os, json, unittest, time, shutil, sys
import h2o_cmd, h2o

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # ssh channel closes if we build_cloud with remote hosts here
        # hosts and nodes have to be accessible between classes otherwise the ssh channels close
        global nodes
        global hosts

        #*************************************
        # modify here
        global nodesPerHost
        global hostsUsername
        global hostsPassword
        global hostsList

        # 4 was bad
        nodesPerHost = 4
        hostsUsername = '0xdiag'
        hostsPassword = '0xdiag'

        if (1==1):
            hostsList = [
                '192.168.1.17',
                '192.168.1.20',
                ]
        elif (1==1):
            hostsList = [
                '192.168.0.33',
                '192.168.0.37',
                ]
        elif (1==1):
            nodesPerHost = 8
            hostsUsername = '0xdiag'
            hostsPassword = '0xdiag'
            hostsList = [
                '192.168.0.35',
                '192.168.0.37',
                ]
        #*************************************
        hosts = []
        for h in hostsList:
            h2o.verboseprint("Connecting to:", h)
            hosts.append(h2o.RemoteHost(h, hostsUsername, hostsPassword))

        h2o.upload_jar_to_remote_hosts(hosts)

        # timeout wants to be larger for large numbers of hosts * nodesPerHost
        # use 60 sec min, 2 sec per node.
        timeoutSecs = max(60, 2*(len(hosts) * nodesPerHost))
        h2o.build_cloud(nodesPerHost,hosts=hosts,timeoutSecs=timeoutSecs)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        # required in every test
        h2o.touch_cloud()

        for n in h2o.nodes:
            h2o.verboseprint("cloud check for:", n)            
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_B_RF_iris2(self):
        h2o.touch_cloud()

        timeoutSecs = max(10, 2*(len(hosts) * nodesPerHost))
        RFview = h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/iris/iris2.csv'))

    def test_C_RF_poker100(self):
        h2o.touch_cloud()

        h2o_cmd.runRF( trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/poker/poker100'))

    def test_D_GenParity1(self):
        h2o.touch_cloud()

        # Create a directory for the created dataset files. ok if already exists
        global SYNDATASETS_DIR
        global SYNSCRIPTS_DIR

        SYNDATASETS_DIR = './syn_datasets'
        if os.path.exists(SYNDATASETS_DIR):
            shutil.rmtree(SYNDATASETS_DIR)
        os.mkdir(SYNDATASETS_DIR)

        SYNSCRIPTS_DIR = './syn_scripts'

        # always match the run below!
        for x in xrange (11,100,10):
            # more rows!
            y = 10000 * x
            # Have to split the string out to list for pipe
            shCmdString = "perl " + SYNSCRIPTS_DIR + "/parity.pl 128 4 "+ str(y) + " quad"
            # FIX! as long as we're doing a couple, you'd think we wouldn't have to 
            # wait for the last one to be gen'ed here before we start the first below.
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=15)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        trees = 6
        timeoutSecs = 20
        # always match the gen above!
        for x in xrange (11,100,10):
            y = 10000 * x
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(y) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            # FIX! TBD do we always have to kick off the run from node 0?
            # random guess about length of time, varying with more hosts/nodes?
            timeoutSecs = 20 + 2*(len(hosts) * nodesPerHost)
            h2o_cmd.runRF( trees=trees, timeoutSecs=10, csvPathname=csvPathname)
            trees += 10

if __name__ == '__main__':
    h2o.unit_main()

