import h2o, h2o_cmd
import h2o_browse as h2b
import time
import webbrowser

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building cloud'
    #h2o.build_cloud(1, java_heap_GB=1, capture_output=False)
    h2o.nodes = [h2o.ExternalH2O()]
    print 'Upload'
    f = h2o.find_file('smalldata/covtype/covtype.20k.data')
    print 'Parse'
    h2o_cmd.parseFile(csvPathname=f, key="covtype")
    print 'kmeans'
    h2o.nodes[0].kmeans('covtype.hex', 7)
    print 'web?'
    webbrowser.open("http://localhost:54323/KMeansProgress.html?destination_key=covtype.kmeans")
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    #h2o.tear_down_cloud()
