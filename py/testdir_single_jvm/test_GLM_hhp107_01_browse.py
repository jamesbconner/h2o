import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

# auto-ignores constant columns? fails with exception below in browser
# ignoring constant column 9
# ignoring constant column 12
# ignoring constant column 25
# ignoring constant column 54
# ignoring constant column 76
# ignoring constant column 91
# ignoring constant column 103
# Exception in thread "Thread-39" java.lang.IllegalStateException
#     at com.google.gson.JsonArray.getAsString(JsonArray.java:124)
#     at water.web.GLM.getModelHTML(GLM.java:282)
#     at water.web.GLM.serveImpl(GLM.java:344)
#     at water.web.H2OPage.serve(H2OPage.java:46)
#     at water.web.H2OPage.serve(H2OPage.java:14)
#     at water.web.Server.serve(Server.java:168)
#     at water.NanoHTTPD$HTTPSession.run(NanoHTTPD.java:387)
#     at java.lang.Thread.run(Thread.java:722)

import h2o, h2o_cmd
import h2o_hosts, h2o_glm
import h2o_browse as h2b
import time

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            # maybe fails more reliably with just 2 jvms?
            h2o.build_cloud(1,java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2="hhp_107_01.data.hex", timeoutSecs=15)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        # build up the parameter string in X
        Y = "106"
        X = ""
        # go right to the big X and iterate on that case
        ### for trial in range(2):
        for trial in range(2):
            print "\nTrial #", trial, "start"
            print "Y:", Y

            start = time.time()
            kwargs = {'Y': Y}
            # error when also done thru the browser
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, browseAlso=True, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)

            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
