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

# can expand this with specific combinations
# I suppose these args will be ignored with old??
argcaseList = [
    {   'X': '0,1,2,3,4,5,6,7,8,9,10,11',
        'Y': 54,
        'case': 1,
        'family': 'gaussian',
        'norm': 'L2',
        'lambda_1': 1.0E-5,
        'max_iter': 50,
        'weight': 1.0,
        'threshold': 0.5,
        'link': 'familyDefault',
        'xval': 0,
        'expand_cat': 0,
        'beta_eps': 1.0E-4 },
    ]

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
        trial = 0
        for argcase in argcaseList:
            print "\nTrial #", trial, "start"
            kwargs = argcase

            # Y will always be there
            print 'Y:', kwargs['Y']

            start = time.time()
            # we get some errors thru the browser that are unique. browseAlso should run the GLM thru the browser?
            # there's no GLM "view" so we should be redoing the GLM there...keep an eye on changes there
            # interesting question for RF..does browseAlso hit RF or just RFView?
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, browseAlso=True, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
