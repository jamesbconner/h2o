import unittest, time, sys
sys.path.extend(['.','..','py'])

print "Needs numpy, rpy2, and R installed. Run on 192.168.171-175"
# FIX! maybe should update to build_cloud_with_hosts to run on 171-175?

import h2o, h2o_cmd, h2o_glm, h2o_util
import numpy as np
from rpy2 import robjects as ro


def glm_R(csvPathname, col_names, formula):
    df = ro.DataFrame.from_csvfile(csvPathname, col_names=col_names, header=False)
    # formula = 'y ~ a+b+c+d' 
    fit = ro.r.glm(formula=ro.r(formula), data=df, family=ro.r('binomial(link="logit")'))

    # print ro.r.summary(fit)
    coef = ro.r.coef(fit)

    # FIX! generalize on len here somehow
    print "intercept     %.5e:"% coef[0]
    print "coefficient 1 %.5e:"% coef[1]
    print "coefficient 2 %.5e:"% coef[2]
    print "coefficient 3 %.5e:"% coef[3]
    print "coefficient 4 %.5e:"% coef[4]
    print "coefficient 5 %.5e:"% coef[5]
    print "coefficient 6 %.5e:"% coef[6]
    print "coefficient 7 %.5e:"% coef[7]
    print "coefficient 8 %.5e:"% coef[8]

    print "\ncoef:", ro.r.coef(fit)
    print "deviance", ro.r.deviance(fit)
    # print "residuals", ro.r.residuals(fit)
    # print "predict", ro.r.predict(fit)
    # print "fitted", ro.r.fitted(fit)
    # print df

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
            ('uis.dat', 'binomial', 8, 5, None),
            ]

        trial = 0
        for (csvFilename, family, y, timeoutSecs, x) in csvFilenameList:
            # FIX! do something about this file munging
            csvPathname1 = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_2.csv'
            h2o_util.file_strip_comments(csvPathname1, csvPathname2)

            csvPathname3 = SYNDATASETS_DIR + '/' + csvFilename + '_3.csv'
            h2o_util.file_strip_trailing_spaces(csvPathname2, csvPathname3)

            csvPathname4 = SYNDATASETS_DIR + '/' + csvFilename + '_4.csv'
            h2o_util.file_spaces_to_comma(csvPathname3, csvPathname4)

            kwargs = {'xval': 0, 'y': y, 'family': family, 'lambda': 1e4, 'link': 'familyDefault'}
            if x is not None:
                kwargs['x'] = x

            start = time.time()
            glm = h2o_cmd.runGLM(csvPathname=csvPathname4, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname3, 'took', time.time() - start, 'seconds'

            # now do it thru R
            # FIX! generalize on formula and col names here
            formula = 'y ~ a+b+c+d+e+f+g+h'
            col_names = ['a','b','c','d','e','f','g','h','y']
            glm_R(csvPathname4, col_names, formula)

            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()




