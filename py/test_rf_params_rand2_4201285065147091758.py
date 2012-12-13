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
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
# FIX! binLimit causes fail if == 1
print "Temporarily not using binLimit=1"
paramDict = {
    'class': [None,54],
    'classWt': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2'],
    'ntree': [1,3,7,23],
    'modelKey': ['modelkeyA', '012345', '__hello'],
    'OOBEE': ['None', 'true', 'false'],
    'gini': [None, 0, 1],
    'depth': [None, 1,10,20,100],
    'binLimit': [None,1,5,10,100,1000],
    'parallel': [None,0,1],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,100],
    'seed': [None,'0','1','11111','19823134','0x1231231'],
    'features': [None,1,3,5,7,9,11,13,17,19,23,37,54]
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

        # for determinism, I guess we should spit out the seed?
        ##### SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        SEED = 4201285065147091758
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
            
            h2o_cmd.runRF(timeoutSecs=70, csvPathname=csvPathname, **kwargs)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
