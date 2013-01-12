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
            # ('cgd.dat', None, 10)
            ('chdage.dat', 'binomial', 2, 5),
            # hangs during parse for some reason
            # ('clslowbwt.dat', 7, 5),
            ('icu.dat', 'binomial', 1, 5),
            ('lowbwt.dat', 'binomial', 1, 5),
            ('lowbwtm11.dat', 'binomial', 1, 5),
            # ('meexp.dat', None, 5),
            ('nhanes3.dat', 'binomial', 15, 5),
            # ('pbc.dat', None, 5),
            # ('pharynx.dat', None, 5),
            ('pros.dat', 'binomial', 1, 5),
            ('uis.dat', 'binomial', 8, 5),
            ]

        trial = 0
        for (csvFilename, family, Y, timeoutSecs) in csvFilenameList:
            csvPathname1 = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_stripped.csv'
            h2o_util.file_strip_trailing_spaces(csvPathname1, csvPathname2)

            kwargs = {'xval': 0, 'case': 'NaN', 'Y': Y, \
                    'family': family, 'norm': 'ELASTIC', 'lambda1': 1e-8, 'link': 'familyDefault'}
            start = time.time()
            glm = h2o_cmd.runGLM(csvPathname=csvPathname2, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname2, 'took', time.time() - start, 'seconds'
            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
