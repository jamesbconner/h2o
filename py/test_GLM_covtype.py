import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        node_count = 4
        h2o.build_cloud(node_count)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)

    def test_A_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_C_hhp_107_01(self):
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
            if (appendX>49):
                sys.stdout.write('.')
                sys.stdout.flush() 
                print "\nX:", X
                print "Y:", Y

                start = time.time()
                ### FIX! add some expected result checking
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, X=X, Y=Y, timeoutSecs=timeoutSecs)

                h2o.verboseprint("\nglm:", glm)
                print "\nerrRate:", glm['errRate']
                print "trueNegative:", glm['trueNegative']
                print "truePositive:", glm['truePositive']
                print "falseNegative:", glm['falseNegative']
                print "falsePositive:", glm['falsePositive']
                print "coefficients:", glm['coefficients']
                print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'



if __name__ == '__main__':
    h2o.unit_main()
