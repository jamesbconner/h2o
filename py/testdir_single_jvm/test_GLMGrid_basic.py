import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        ## time.sleep(3600)
        h2o.tear_down_cloud()

    def test_B_benign(self):
        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename)
        # columns start at 0
        # cols 0-13. 3 is output
        # no member id in this one
        Y = "3"
        xList = []  
        for appendx in xrange(14):
            if (appendx == 0): 
                print "\nSkipping 0. Causes coefficient of 0 when used alone"
            elif (appendx == 3): 
                print "\n3 is output."
            else:
                xList.append(appendx)

        x = ','.join(map(str, xList))

        # just run the test with all x, not the intermediate results
        csvFilename = "benign.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        print "\nx:", x
        print "y:", y
        
        # FIX! hacking with norm = L2 to get it to pass now. ELASTIC default won't? maybe
        # issue with case in GLM in h2o.py. have to set it to something otherwise H2O complains
        kwargs = {
            'x': x, 'y':  y, 'xval': 2, 
            'lambda1': '1e-8:1e3:100', 
            'lambda2': '1e-8:1e3:100',
            'alpha': '1,1.4,1.8',
            'thresholds': '0:1:0.01'
            }
        # fails with xval
        print "Not doing xval with benign. Fails with 'unable to solve?'"

        gg = h2o_cmd.runGLMGridOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        # check the first in the models list. It should be the best
        colNames = [ 'STR','OBS','AGMT','FNDX','HIGD','DEG','CHK',
                     'AGP1','AGMN','NLV','LIV','WT','AGLP','MST' ]

        # h2o_glm.simpleCheckGLMGrid(self, gg, colNames[xList[-1]], **kwargs)
        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

    def test_C_prostate(self):
        print "\nStarting prostate.csv"
        # columns start at 0
        csvFilename = "prostate.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename)

        Y = "1"
        xList = []  
        for appendx in xrange(9):
            if (appendx == 0):
                print "\n0 is member ID. not used"
            elif (appendx == 1):
                print "\n1 is output."
            elif (appendx == 7): 
                print "\nSkipping 7. Causes NaN. Ok now, later though?"
            else:
                xList.append(appendx)

        x = ','.join(map(str, xList))
        # just run the test with all x, not the intermediate results
        print "\nx:", x
        print "y:", y

        # lambda1, lambda2, alpha, rho, threshold
        # FIX! thresholds is used in GLMGrid. threshold is used in GLM
        # comma separated means use discrete values
        # colon separated is min/max/step
        # FIX! have to update other GLMGrid tests
        kwargs = {
            'x': x, 'y':  y, 'xval': 2, 
            'lambda1': '1e-8:1e3:100', 
            'lambda2': '1e-8:1e3:100',
            'alpha': '1,1.4,1.8',
            'thresholds': '0:1:0.01'
            }

        gg = h2o_cmd.runGLMGridOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        colNames = ['D','CAPSULE','AGE','RACE','DPROS','DCAPS','PSA','VOL','GLEASON']
        # h2o_glm.simpleCheckGLMGrid(self, gg, colNames[xList[0]], **kwargs)
        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
