import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

# the shared exec expression creator and executor
import h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        for j in range(colCount):
            ri1 = int(r1.triangular(0,2,1.5))
            rowData.append(str(ri1))
            rowTotal += ri1

        # sum the row, and make output 1 if > (5 * rowCount)
        if (rowTotal > (.7 * colCount)): 
            result = 1
        else:
            result = 0
        rowData.append(str(result))
        # add the output twice, to try to match to it?
        rowData.append(str(result))
        ### print colCount, rowTotal, result
        rowDataCsv = ",".join(rowData)
        dsf.write(rowDataCsv + "\n")

    dsf.close()



paramDict = {
    # 'key': ['cA'],
    'y': [11],
    'family': ['binomial'],
    # 'norm': ['ELASTIC'],
    'norm': ['L2'],
    'lambda_1': [1.0E-5],
    'lambda_2': [1.0E-8],
    'alpha': [1.0],
    'rho': [0.01],
    'max_iter': [50],
    'weight': [1.0],
    'threshold': [0.5],
    # 'case': [NaN],
    'case': ['NaN'],
    # 'link': [familyDefault],
    'xval': [1],
    'expand_cat': [1],
    'beta_eps': [1.0E-4],
    }

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
            h2o.build_cloud(1,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()


    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

# http://192.168.1.171:55322/GLM.query?


    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000000,  10, 'cA', 300),
            (1000000,  20, 'cB', 300),
            (1000000,  30, 'cC', 300),
            (1000000,  40, 'cD', 300),
            (1000000,  50, 'cE', 300),
            (1000000,  60, 'cF', 300),
            (1000000,  70, 'cG', 300),
            (1000000,  80, 'cH', 300),
            (1000000,  90, 'cI', 300),
            (1000000, 100, 'cJ', 300),
            (1000000, 200, 'cK', 300),
            (1000000, 300, 'cL', 300),
            (1000000, 400, 'cM', 300),
            (1000000, 500, 'cN', 300),
            (1000000, 600, 'cO', 300),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "\nParse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            paramDict2 = {}
            for k in paramDict:
                paramDict2[k] = paramDict[k][0]

            Y = colCount - 1
            kwargs = {'Y': Y, 'case': 'NaN'}
            kwargs.update(paramDict2)

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 8, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
