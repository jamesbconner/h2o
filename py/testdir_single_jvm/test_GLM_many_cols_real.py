import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generatators, 1 per column
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            # fails with just randint 0,1
            # r = r1.randint(0,1)
            # ri1 = r1.randint(0,1)
            ri1 = int(r1.gauss(1,.1))
            # ri2 = r2.randint(0,20)
            # no NA
            ri2 = 1

            # 5% NA
            if (ri2==0):
                # rs = ""
                r = ri1
            else:
                r = ri1

            rowData.append(r + 0.1)

        # sum the row, and make output 1 if > (5 * rowCount)
        rowTotal = sum(rowData)
        if (rowTotal > (0.5 * colCount)): 
            result = 1
        else:
            result = 0

        rowData.append(result)
        # add the output twice, to try to match to it?
        rowData.append(result)
        ###  print colCount, rowTotal, result
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
        h2o.tear_down_cloud()

    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000, 101, 'cA', 300),
            (1000, 201, 'cB', 300),
            (1000, 301, 'cC', 300),
            (1000, 401, 'cD', 300),
            (1000, 501, 'cE', 300),
            (1000, 601, 'cF', 300),
            (1000, 701, 'cG', 300),
            (1000, 801, 'cH', 300),
            (1000, 901, 'cI', 300),
            (1000, 1001, 'cJ', 300),
            (1000, 2001, 'cK', 300),
            (1000, 3001, 'cL', 300),
            (1000, 4001, 'cM', 300),
            (1000, 5001, 'cN', 300),
            (100, 6001, 'cO', 300),
            (100, 7001, 'cP', 300),
            (100, 8001, 'cQ', 300),
            (100, 9001, 'cR', 300),
            (100, 10001, 'cS', 300),
            (100, 11001, 'cT', 300),
            ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            Y = colCount - 1
            kwargs = {'Y': Y, 'iterations': 10, 'case': '0.1', 'norm': 'L2'}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)

            # try new offset/view
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=100, view=100)


if __name__ == '__main__':
    h2o.unit_main()
