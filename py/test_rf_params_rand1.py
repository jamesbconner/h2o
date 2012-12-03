import unittest
import h2o, h2o_cmd
import random, sys

# RF?
# classWt=&
# class=2&
# ntree=&
# modelKey=& 
# OOBEE=true&
# gini=1&
# depth=&
# binLimit=&
# parallel=&
# ignore=&   ...this is ignore columns
# sample=&
# seed=&
# features=1&
# singlethreaded=0&     ..debug only..may be gone
# Key=chess_2x2_500_int.hex

# make a dict of lists, with some legal choices for each. None means no value.
# assume poker1000 datset

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 0-8 in the last col of poker1000
paramDict = {
    'class': [None,10],
    'classWt': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2','8=2'],
    'ntree': [None,1,10,100],
    'modelKey': ['modelkeyA', '012345', '__hello'],
    'OOBEE': ['None', 'true', 'false'],
    'gini': [None, 0, 1],
    'depth': [None, 1,10,20,100],
    'binLimit': [None,1,5,10,100,1000],
    'parallel': [None,0,1],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,100],
    'seed': [None,'0','1','11111','19823134','0x1231231'],
    'features': [None,1,2,3,4,5,6,7,8,9]
    }

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_poker1000(self):
        csvPathname = '../smalldata/poker/poker1000'

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        for trial in range(20):
            # form random selections of RF parameters
            kwargs = {}
            randomGroupSize = random.randint(1,len(paramDict))
            for i in range(randomGroupSize):
                randomKey = random.choice(paramDict.keys())
                randomV = paramDict[randomKey]
                randomValue = random.choice(randomV)
                kwargs[randomKey] = randomValue

            print kwargs
            
            h2o_cmd.runRF(timeoutSecs=20, csvPathname=csvPathname, **kwargs)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
