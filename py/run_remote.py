import h2o, cmd

nodes_per_host = 2
hosts = [
    h2o.RemoteHost('rufus.local', 'fowles'),
    h2o.RemoteHost('eiji.local', 'boots'),
]


for h in hosts:
    print 'Uploading jar to', h
    h.upload_file(h2o.find_file('build/h2o.jar'))

nodes = []
for h in hosts:
    for i in xrange(nodes_per_host):
        print 'Starting node', i, 'via', h
        nodes.append(h.remote_h2o(port=54321 + i*3))

print 'Stabilize'
h2o.stabilize_cloud(nodes[0], len(nodes))

print 'Random Forest'
cmd.runRF(nodes[0], h2o.find_file('smalldata/poker/poker-hand-testing.data'),
        trees=10, timeoutSecs=60)
print 'Completed'

