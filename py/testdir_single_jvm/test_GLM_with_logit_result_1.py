import unittest
import random, sys, time, os, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm


def generate_rand_equation(colCount, SEED):
    r1 = random.Random(SEED)
    coefficients = []
    # y = 1/(1 + exp(-(sum(coefficients*x)+intercept))
    for j in range(colCount):
        # ri = r1.randint(-1,1)
        rif = r1.uniform(0,1)
        # FIX! temporary try fixed = col+1
        # coefficients.append(rif)
        coefficients.append(j+1)
        # coefficients.append(2 + 2*(j%2))


    ### ri = r1.randint(-1,1)
    ### intercept = (ri)
    intercept =  -0.25
    intercept =  0
    print "Expected coefficients:", coefficients
    print "Expected intercept:", intercept

    return(coefficients, intercept) 

# FIX! random noise on coefficients? randomly force 5% to 0?  
#y = 1/(1 + math.exp(-(sum(coefficients*x)+intercept)) 
def generate_binomial_from_eqn_and_data(coefficients, intercept, rowData, noise=None):
    # FIX! think about using noise on some of the rowData
    cx = [a*b for a,b in zip(coefficients, rowData)]
    y = 1/(1 + math.exp(-(sum(cx) + intercept)))
    # y should be between 0 and 1

    ### print y
    if (y<0 or y>1):
        raise Exception("Generated y result is should be between 0 and 1: " + y)
    if (y>=0.75):
        return (1,y)
    else:
        return (0,y)

def write_syn_dataset(csvPathname, rowCount, colCount, coefficients, intercept, SEED, noise=None):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    # assuming output is always last col
    minActual = None  
    maxActual = None
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1) 
            # ri = r1.triangular(0,1,j/colCount)
            rowData.append(ri)

        (binomial, actual) = generate_binomial_from_eqn_and_data(coefficients, intercept, rowData, noise=noise)
        if minActual is None or actual<minActual: minActual = actual
        if maxActual is None or actual>maxActual: maxActual = actual

        rowData.append(binomial)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    print "minActual:", minActual, " maxActual:", maxActual


class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000, 5, 'cA', 300), 
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname, "using random coefficients and intercept and logit eqn. for output"
            (coefficients, intercept) = generate_rand_equation(colCount, SEEDPERFILE)
            print coefficients, intercept
            write_syn_dataset(csvPathname, rowCount, colCount, coefficients, intercept, SEEDPERFILE)


            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {'y': y, 'max_iter': 60, 
                    'penalty': 10,
                    'alpha': 1.0,
                    'weight': 1.0,
                    'link': 'familyDefault',
                    # what about these?
                    # 'link': [None, 'logit','identity', 'log', 'inverse'],
                    'xval': 3,
                    'beta_eps': 1e-4,
                    'thresholds': 0.5,
                    }

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, 0, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
