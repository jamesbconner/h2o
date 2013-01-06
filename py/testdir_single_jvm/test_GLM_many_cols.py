import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

# the shared exec expression creator and executor
import h2o_exec as h2e

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
            ri1 = r1.randint(0,1)
            # ri2 = r2.randint(0,20)
            # FIX! no NAs allowed for now (rows are thrown out!)
            rs = str(ri1)
            rowData.append(rs)

        rowDataCsv = ",".join(rowData)
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
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()


    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()


    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # (100, 1000, 'cA', 50),
            # (100, 2000, 'cB', 50),
            # (100, 3000, 'cC', 50),
            # (100, 4000, 'cD', 50),
            # (100, 5000, 'cE', 50),
            # (100, 6000, 'cF', 50),
            # (100, 7000, 'cG', 50),
            # (100, 8000, 'cH', 50),
            # (100, 9000, 'cI', 50),
            # (100, 2000, 'cJ', 50),
            # (100, 3000, 'cK', 100), # 97 secs total
            (100, 3000, 'cK', 300),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            Y = colCount - 1

            # FIX! what are the legal values for case? is it one of the values in the output? or
            # the encoded value or ??
            # {u'error': u'Argument case error: Value -1.0 is not between 0.0 and 1.0 (inclusive)'}
            kwargs = {'Y': Y, 'norm': 'L2', 'max_iter': 50, 'case': 'NaN'}
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)

            # try new offset/view
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=100, view=100)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=99, view=89)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=-1, view=53)


if __name__ == '__main__':
    h2o.unit_main()
