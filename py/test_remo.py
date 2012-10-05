import h2o as h2o, cmd
# import logging
# logging.basicConfig()
import time

h2o.clean_sandbox()
h2o.parse_our_args()

nodes_per_host = 2
hosts = [
    # h2o.RemoteHost('rufus.local', 'fowles'),
    # h2o.RemoteHost('eiji.local', 'boots'),
    # h2o doesn't like getting KevinVB on the ip= ..
    # should get the ip address like the normal tests?
    # h2o.RemoteHost('192.168.0.33', 'kevin', 'border'),
    # h2o.RemoteHost('192.168.0.37', 'kevin', 'border'),

    h2o.RemoteHost('192.168.1.150', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.151', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.152', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.153', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.154', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.155', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.156', '0xdiag', '0xdiag'),
    h2o.RemoteHost('192.168.1.160', '0xdiag', '0xdiag'),

]


# pulling this out front, because I want all the nodes
# to come up quickly and fight with each other
for h in hosts:
    print 'Uploading jar to', h
    h.upload_file(h2o.find_file('build/h2o.jar'))

nodes = []
for h in hosts:
    for i in xrange(nodes_per_host):
        print 'Starting node', i, 'via', h
        # kbn. temp hack changing port # to avoid sri's use of 54321 + i*3
        nodes.append(h.remote_h2o(port=55321 + i*3))

print 'Stabilize'
start = time.time()
h2o.stabilize_cloud(nodes[0], len(nodes))
print len(nodes), " nodes stabilized in ", time.time()-start, " secs"

print 'Random Forest'
cmd.runRF(nodes[0], h2o.find_file('smalldata/poker/poker-hand-testing.data'),
        trees=10, timeoutSecs=60)
print 'Completed'

