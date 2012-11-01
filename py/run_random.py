import h2o, h2o_cmd, ec2
import h2o_browse as h2b
import time

h2o.clean_sandbox()
h2o.parse_our_args()

reservation = ec2.run_instances(2)
try:
    print 'Building cloud'
    hosts = ec2.create_hosts(reservation)
    ec2.build_cloud(hosts, 2, capture_output=False, java_heap_GB=15)
    print 'Random Forest'
    h2o_cmd.runRF(None, h2o.find_file('smalldata/iris/iris2.csv'))
    print 'Completed'
    h2b.browseJsonHistoryAsUrlLastMatch("RFView")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    ec2.terminate_instances(reservation)
