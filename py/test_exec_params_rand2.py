import unittest
import h2o, h2o_cmd, h2o_browse as h2b
import random, sys, time

# none is illegal for threshold
# always run with xval, to make sure we get the trainingErrorDetails
# family gaussian gives us there
# Exception in thread "Thread-6" java.lang.RuntimeException: Matrix is not symmetric positive definite.
# at Jama.CholeskyDecomposition.solve(CholeskyDecomposition.java:173)

#    'Key' : key,

paramDict = {
    'min' : [None], # (col)
    'max' : [None], # (col)
    'sum' : [None], # (col)
    'mean' : [None], # (col)
    'filter': [None], # (col)
    'slice': [None], # (mean)
    'randomBitVector': [None], # (srcFrame,col)
    'randomFilter': [None], # (srcFrame,startcol,count=)
    'log': [None], # (col)
    'colSwap': [None], # (sourcekey, col, newcol)
    'makeEnum': [None], # (vector)
    }

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(None, csvPathname, 'covtype.data', 'covtype.hex', 10000)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        h2b.browseTheCloud()
        for trial in range(53):
            # default
            colX = trial
            # form random selections of RF parameters
            # always need Y=54. and always need some xval (which can be overwritten)
            # with a different choice. we need the xval to get the error details 
            # in the json(below)
            # force family=binomial to avoid the assertion error above with gaussian
            exec_expr2 = {'Y': 54}
            randomGroupSize = random.randint(1,len(paramDict))
            for i in range(randomGroupSize):
                randomKey = random.choice(paramDict.keys())
                randomV = paramDict[randomKey]
                randomValue = random.choice(randomV)
                exec_expr2[randomKey] = randomValue

                if (randomKey=='X'):
                    # keep track of what column we're picking
                    colX = randomValue

# colSwap(all.hex,34,(all.hex[34] == 0 ? 0 : 1)
# b=colSwap(claim.hex,34,makeEnum(claim.hex[34] == 0 ? 0 : 1)) Log Transformation of Claims Column
# e=colSwap(claim.hex,34,makeEnum(claim.hex[34] <10 ? 0 : (claim.hex[34] <100 ? 1 : (claim.hex[34] <1000 ? 2 : 3))))

            # FIX! maybe we'll just keep a list? no..just do a random selection of one command?
            # I suppose we should put in varying before somehow
            exec_expr = "colSwap(covtype.hex,2,(covtype.hex[2]==0 ? 0 : 1))"
            exec_expr = "colSwap(covtype.hex," + str(colX) + ",(covtype.hex[2]==0 ? 54321 : 54321))"
            print exec_expr
            
            start = time.time()
            print "Parse key is:", parseKey['Key']
            exec_result = h2o_cmd.runExecOnly(parseKey=parseKey, timeoutSecs=70, Expr=exec_expr)
            print h2o.dump_json(exec_result)
            h2b.browseJsonHistoryAsUrlLastMatch("Exec")
            time.sleep(1.5)

            resultInspect = h2o.nodes[0].inspect(exec_result['ResultKey'])
            h2o.verboseprint(h2o.dump_json(resultInspect))

            print "exec end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

            # use the result as the next thing to work on?
            parseKey['Key'] = exec_result['ResultKey']
 

if __name__ == '__main__':
    h2o.unit_main()
