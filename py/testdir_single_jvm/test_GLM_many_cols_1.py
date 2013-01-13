import unittest
import random, sys, time, os, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowTotal = 0
        for j in range(colCount):
            ri1 = int(r1.gauss(1,.1))
            rowData.append(ri1)

        result = r2.randint(0,1)
        rowData.append(str(result))
        # add the output twice, to try to match to it?
        # Hauck Donner effect. Can't have copy of output in the input??
        # http://kups.ku.edu/maillist/classes/ps707/2005/msg00023.html
        ### print colCount, rowTotal, result
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

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

    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # (1000, 101, 'cA', 300),
            # (1000, 201, 'cB', 300),
            # (1000, 301, 'cC', 300),
            # (1000, 401, 'cD', 300),
            (1000, 501, 'cE', 300),
            (1000, 601, 'cF', 300),
            (1000, 701, 'cG', 300),
            (1000, 801, 'cH', 300),
            (1000, 901, 'cI', 300),
            (1000, 1001, 'cJ', 300),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            y = colCount
            kwargs = {'y': y, 'max_iter': 50, 'case': 'NaN', 'norm': 'ELASTIC'}

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # we can pass the warning, without stopping in the test, so we can 
            # redo it in the browser for comparison
            warnings = h2o_glm.simpleCheckGLM(self, glm, 13, allowFailWarning=True, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                time.sleep(5)

            # gets the failed to converge, here, after we see it in the browser too
            x = re.compile("[Ff]ailed")
            if warnings:
                for w in warnings:
                    if (re.search(x,w)): raise Exception(w)

if __name__ == '__main__':
    h2o.unit_main()
