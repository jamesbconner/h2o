import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ### ri1 = int(r1.triangular(0,2,1.5))
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        if translateList is not None:
            for i, iNum in enumerate(rowData):
                rowData[i] = translateList[iNum]

        ### if (rowTotal > (.7 * colCount)): 
        if (rowTotal > (1.6 * colCount)): 
            result = 1
        else:
            result = 0

        ### print colCount, rowTotal, result

        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        # add the output twice, to try to match to it?
        rowDataStr.append(str(result))

        rowDataCsv = ",".join(rowDataStr)
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
    # 'case': [None],
    # 'link': [familyDefault],
    'xval': [2],
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
            h2o.build_cloud(3,java_heap_GB=8,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (10000000,  10, 'cA', 300),
            (10000000,  50, 'cB', 600),
            (10000000,  100, 'cC', 900),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            # kwargs = {'Y': Y, 'norm': 'L2', 'iterations': 10, 'case': 1}
            paramDict2 = {}
            for k in paramDict:
                paramDict2[k] = paramDict[k][0]

            # since we add the output twice, it's no longer colCount-1
            Y = colCount+1
            kwargs = {'Y': Y, 'max_iter': 50, 'case': 1}
            kwargs.update(paramDict2)

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # only col Y-1 (next to last)doesn't get renamed in coefficients due to enum/categorical expansion
            print "Y:", Y 
            # FIX! bug was dropped coefficients if constant column is dropped
            ### h2o_glm.simpleCheckGLM(self, glm, Y-2, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                time.sleep(15)
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(15)

            # try new offset/view
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=100, view=100)
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=99, view=89)
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=-1, view=53)

if __name__ == '__main__':
    h2o.unit_main()
