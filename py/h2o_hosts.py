
import getpass, json, h2o
# UPDATE: all multi-machine testing will pass list of IP and base port addresses to H2O
# means we won't realy on h2o self-discovery of cluster

def build_cloud_with_hosts():
    # For seeing example of what we want in the json, if we add things
    #   import h2o_config

    # loading json for config
    configFilename = './pytest_config-%s.json' %getpass.getuser()
    h2o.verboseprint("Loading host config from", configFilename)
    with open(configFilename, 'rb') as fp:
         hostDict = json.load(fp)

    hostList = hostDict.setdefault('ip','192.168.0.161')
    h2oPerHost = hostDict.setdefault('h2o_per_host', 2)
    username = hostDict.setdefault('username','0xdiag')
    # stupid but here for clarity
    password = hostDict.setdefault('password', None)
    h2o.verboseprint("host config: ", username, password, h2oPerHost, hostList)

    #*************************************
    global hosts
    hosts = []
    for h in hostList:
        h2o.verboseprint("Connecting to:", h)
        hosts.append(h2o.RemoteHost(h, username, password))

    h2o.upload_jar_to_remote_hosts(hosts)

    # timeout wants to be larger for large numbers of hosts * h2oPerHost
    # use 60 sec min, 2 sec per node.
    timeoutSecs = max(60, 2*(len(hosts) * h2oPerHost))
    h2o.build_cloud(h2oPerHost,base_port=55321,hosts=hosts,timeoutSecs=timeoutSecs)
