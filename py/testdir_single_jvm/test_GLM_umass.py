import unittest, time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_util

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
        print "\nWARNING: I'm stripping trailing/leading spaces on datasets into syn_datasets to work around parser"
        # filename, Y, timeoutSecs
        # fix. the ones with comments may want to be a gaussian?
        csvFilenameList = [
            ('cgd.dat', 'gaussian', 12, 5, None),
            ('chdage.dat', 'binomial', 2, 5, None),
    
            # leave out ID and birth weight
            ('clslowbwt.dat', 'binomial', 7, 5, '1,2,3,4,5'),
            ('icu.dat', 'binomial', 1, 5, None),
            # need to exclude col 0 (ID) and col 10 (bwt)
            # but -x doesn't work..so do 2:9...range doesn't work? FIX!
            ('lowbwt.dat', 'binomial', 1, 5, '2,3,4,5,6,7,8,9'),
            ('lowbwtm11.dat', 'binomial', 1, 5, None),
            ('meexp.dat', 'gaussian', 3, 5, None),
            ('nhanes3.dat', 'binomial', 15, 5, None),
            ('pbc.dat', 'gaussian', 1, 5, None),
            ('pharynx.dat', 'gaussian', 12, 5, None),
            ('pros.dat', 'binomial', 1, 5, None),
            ('uis.dat', 'binomial', 8, 5, None),
            ]

        trial = 0
        for (csvFilename, family, y, timeoutSecs, x) in csvFilenameList:
            csvPathname1 = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_stripped.csv'
            h2o_util.file_strip_trailing_spaces(csvPathname1, csvPathname2)

            kwargs = {'xval': 0, 'case': 'NaN', 'y': y, \
                
                    'family': family, 'norm': 'NONE', 'rho': 10, 'lambda1': 1e4, 'link': 'familyDefault'}
            if x is not None:
                kwargs['x'] = x

            start = time.time()
            glm = h2o_cmd.runGLM(csvPathname=csvPathname2, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname2, 'took', time.time() - start, 'seconds'
            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
