import unittest
import h2o, h2o_cmd, h2o_hosts
import os, time, sys


def runLinuxCmds(cmds):
    for c in cmds:
        h2o.verboseprint(c)
        os.system("sudo " + c)

def showIptables():
    print "\nshowing iptables -L now"
    cmds = ["iptables -L"]
    runLinuxCmds(cmds)

def allAcceptIptables():
    print "Stopping firewall and allowing everyone..."
    cmds = ["iptables -F"]
    cmds.append("iptables -X")
    cmds.append("iptables -t nat -F")
    cmds.append("iptables -t nat -X")
    cmds.append("iptables -t mangle -F")
    cmds.append("iptables -t mangle -X")
    cmds.append("iptables -P INPUT ACCEPT")
    cmds.append("iptables -P FORWARD ACCEPT")
    cmds.append("iptables -P OUTPUT ACCEPT")
    runLinuxCmds(cmds)

# not used right now
def allAcceptIptablesMethod2():
    print "Stopping firewall and allowing everyone..."
    cmds = ["iptables -flush"]
    cmds.append("iptables -delete-chain")
    cmds.append("iptables -table filter -flush")
    cmds.append("iptables -table nat -delete-chain")
    cmds.append("iptables -table filter -delete-chain")
    cmds.append("iptables -table nat -flush")

def multicastAcceptIptables():
    print "Enabling Multicast (only), send and receive"
    cmds = ["iptables -A INPUT  -m pkttype --pkt-type multicast -j ACCEPT"]
    cmds.append("iptables -A OUTPUT -m pkttype --pkt-type multicast -j ACCEPT")
    cmds.append("iptables -A INPUT  --protocol igmp -j ACCEPT")
    cmds.append("iptables -A OUTPUT --protocol igmp -j ACCEPT")
    cmds.append("iptables -A INPUT  --dst '224.0.0.0/4' -j ACCEPT")
    cmds.append("iptables -A OUTPUT --dst '224.0.0.0/4' -j ACCEPT")
    runLinuxCmds(cmds)

def multicastDropIptables():
    print "Disabling Multicast (only), send and receive"
    cmds = ["iptables -A INPUT  -m pkttype --pkt-type multicast -j DROP"]
    cmds.append("iptables -A OUTPUT -m pkttype --pkt-type multicast -j DROP")
    cmds.append("iptables -A INPUT  --protocol igmp -j DROP")
    cmds.append("iptables -A OUTPUT --protocol igmp -j DROP")
    cmds.append("iptables -A INPUT  --dst '224.0.0.0/4' -j DROP")
    cmds.append( "iptables -A OUTPUT --dst '224.0.0.0/4' -j DROP")
    runLinuxCmds(cmds)

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        global nodes_per_host
        # FIX! will this override the json that specifies the multi-host stuff
        nodes_per_host = 4

        print "\nThis test may prompt for sudo passwords for executing iptables on linux"
        print "Don't execute as root. or without understanding 'sudo iptables' and this video"
        print "\nhttp://www.youtube.com/watch?v=OWwOJlOI1nU"

        print "\nIt will use pytest_config-<username>.json for multi-host"
        print "Want to run with multicast send or receive disabled somehow in the target cloud"
        print "The test tries to do it with iptables"

        print "\nIf the test fails, you'll have to fix iptables to be how you want them"
        print "Typically 'iptables -F' should be enough."

        print "\nDon't run this on 192.168.1.151 which has sshguard enabled in its iptables"
        print "Only for linux. hopefully centos and ubuntu. don't know about mac"

        print "\nNormally you'll want this to run with -v to see hangs in cloud building"

        print "\nThis is how iptables is at start:"
        showIptables()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        allAcceptIptables()
        showIptables()

    def test_A_build_cloud_with_hosts(self):
        print "\nno flatfile, Build allowing anything"
        allAcceptIptables()
        showIptables()
        h2o_hosts.build_cloud_with_hosts(nodes_per_host, use_flatfile=False)
        h2o.tear_down_cloud()

    def test_B_build_cloud_with_hosts(self):
        print "\nwith flatfile, Build allowing anything"
        allAcceptIptables()
        showIptables()
        h2o_hosts.build_cloud_with_hosts(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()


    def test_C_build_cloud_with_hosts_no_multicast(self):
        print "\nwith flatfile, with multicast disabled"
        allAcceptIptables()
        showIptables()

        multicastDropIptables()
        h2o_hosts.build_cloud_with_hosts(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()

    def test_D_build_cloud_with_hosts_no_multicast(self):
        print "\nwith flatfile, with multicast disabled, and RF, 5 trials"
        allAcceptIptables()
        showIptables()

        multicastDropIptables()
        csvPathname = '../smalldata/poker/poker1000'

        for x in range(1,5):
            h2o_hosts.build_cloud_with_hosts(nodes_per_host, use_flatfile=True)
            h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)
            h2o.tear_down_cloud()
            h2o.verboseprint("Waiting", nodes_per_host,
                "seconds to avoid OS sticky port problem")
            time.sleep(nodes_per_host)
            print "Trial", x
            sys.stdout.write('.')
            sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
