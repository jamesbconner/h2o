import unittest, time, sys
sys.path.extend(['.','..','py'])

print "Needs numpy, rpy2, and R installed. Run on 192.168.171-175"
# FIX! maybe should update to build_cloud_with_hosts to run on 171-175?

import h2o, h2o_cmd, h2o_glm, h2o_util
import numpy as np
from rpy2 import robjects as ro


def glm_R(csvPathname, col_names, formula):
    # df = ro.DataFrame.from_csvfile(csvPathname, col_names=col_names, header=False)
    df = ro.DataFrame.from_csvfile(csvPathname, header=False)
    cn = ro.r.colnames(df)
    print cn
    # print df
    fit = ro.r.glm(formula=ro.r(formula), data=df, family=ro.r('binomial(link="logit")'))

    # print ro.r.summary(fit)
    coef = ro.r.coef(fit)

    print "intercept     %.5e:"% coef[0]
    for i in range(1,len(coef)):
        print "coefficient", i, "%.5e" % coef[i]

    print "\ncoef:", ro.r.coef(fit)
    print "deviance", ro.r.deviance(fit)
    # print "residuals", ro.r.residuals(fit)
    # print "predict", ro.r.predict(fit)
    # print "fitted", ro.r.fitted(fit)

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_umass(self):
        csvFilenameList = [
            ('uis.dat', 'binomial', 8, 5, None, 'V9 ~ V1+V2+V3+V4+V5+V6+V7+V8')
        ]

        trial = 0
        for (csvFilename, family, y, timeoutSecs, x, formula) in csvFilenameList:
            # FIX! do something about this file munging
            csvPathname1 = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_2.csv'
            h2o_util.file_clean_for_R(csvPathname1, csvPathname2)

            kwargs = {'xval': 0, 'y': y, 'family': family, 'link': 'familyDefault',
                'alpha': 0, 'lambda': 0}

            if x is not None:
                kwargs['x'] = x

            start = time.time()
            glm = h2o_cmd.runGLM(csvPathname=csvPathname2, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname2, 'took', time.time() - start, 'seconds'

            # now do it thru R
            # generalize on formula and col names here
            # col_names = ['a','b','c','d','e','f','g','h','y']
            
            glm_R(csvPathname2, None, formula)

            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()




