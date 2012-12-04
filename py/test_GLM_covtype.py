import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uses two much memory with 4?
        h2o.build_cloud(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_B_covtype(self):
        timeoutSecs = 2
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        print "\n" + csvPathname

        # columns start at 0
        Y = "54"
        X = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)

        for appendX in xrange(55):
            # if (appendX == 9):
            # elif (appendX == 12): 
            # else:
            # all cols seem good so far (up to 32 tested..then a different DKV fail.
            if (1==1):
                if X == "": 
                    X = str(appendX)
                else:
                    X = X + "," + str(appendX)

            # only run if appendX is > 49 to save time
            # we need to cycle up to there though, to get the parameters right for GLM json
            # if (appendX>49):
                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                ### FIX! add some expected result checking
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, xval=11, X=X, Y=Y, timeoutSecs=timeoutSecs)

                # different when xvalidation is used? No trainingErrorDetails?
                h2o.verboseprint("\nglm:", glm)
                if 'warnings' in glm:
                    print "\nwarnings:", glm['warnings']

                print "GLM time", glm['time']
                print "coefficients:", glm['coefficients']

                tsv = glm['trainingSetValidation']
                print "\ntrainingSetErrorRate:", tsv['trainingSetErrorRate']

                # ted = glm['trainingErrorDetails']
                # print "trueNegative:", ted['trueNegative']
                # print "truePositive:", ted['truePositive']
                # print "falseNegative:", ted['falseNegative']
                # print "falsePositive:", ted['falsePositive']

                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'



if __name__ == '__main__':
    h2o.unit_main()
