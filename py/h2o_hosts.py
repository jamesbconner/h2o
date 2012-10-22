import getpass, json, h2o
# UPDATE: all multi-machine testing will pass list of IP and base port addresses to H2O
# means we won't realy on h2o self-discovery of cluster

# None means the json will specify, or the default for json below
# only these two args override for now. can add more.
def build_cloud_with_hosts(node_count=None, use_flatfile=False, 
    use_hdfs=False, hdfs_name_node="192.168.1.151", **kwargs):

    # For seeing example of what we want in the json, if we add things
    #   import h2o_config

    configFilename = './pytest_config-%s.json' %getpass.getuser()
    h2o.verboseprint("Loading host config from", configFilename)
    with open(configFilename, 'rb') as fp:
         hostDict = json.load(fp)

    hostList = hostDict.setdefault('ip','192.168.0.161')
    h2oPerHost = hostDict.setdefault('h2o_per_host', 2)
    # default should avoid colliding with sri's demo cloud ports: 54321
    basePort = hostDict.setdefault('base_port', 55321)
    username = hostDict.setdefault('username','0xdiag')
    # stupid but here for clarity
    password = hostDict.setdefault('password', None)
    sigar = hostDict.setdefault('sigar', False)

    useFlatfile = hostDict.setdefault('use_flatfile', False)

    useHdfs = hostDict.setdefault('use_hdfs', False)
    hdfs_name_node = hostDict.setdefault('hdfs_name_node', '192.168.1.151')

    java_heap_GB = hostDict.setdefault('java_heap_GB', 4)

    # can override the json with a caller's argument
    # FIX! and we support passing othe kwargs from above? but they don't override
    # json, ...so have to fix here if that's desired
    if node_count is not None:
        h2oPerHost = node_count

    if use_flatfile is not None:
        useFlatfile = use_flatfile

    if use_hdfs is not None:
        useHdfs = use_hdfs

    if hdfs_name_node is not None:
        hdfsNameNode = hdfs_name_node

    h2o.verboseprint("host config: ", username, password, 
        h2oPerHost, basePort, sigar, useFlatfile, 
        useHdfs, hdfsNameNode, hostList, **kwargs)

    #********************
    global hosts
    h2o.verboseprint("About to RemoteHost, likely bad ip if hangs")
    hosts = []
    for h in hostList:
        h2o.verboseprint("Connecting to:", h)
        hosts.append(h2o.RemoteHost(h, username, password))

    h2o.upload_jar_to_remote_hosts(hosts)

    # timeout wants to be larger for large numbers of hosts * h2oPerHost
    # use 60 sec min, 2 sec per node.
    timeoutSecs = max(60, 2*(len(hosts) * h2oPerHost))

    h2o.build_cloud(h2oPerHost,
            base_port=basePort,hosts=hosts,timeoutSecs=timeoutSecs,sigar=sigar, 
            use_flatfile=useFlatfile,
            use_hdfs=useHdfs,hdfs_name_node=hdfsNameNode,
            java_heap_GB=java_heap_GB,
            **kwargs)
