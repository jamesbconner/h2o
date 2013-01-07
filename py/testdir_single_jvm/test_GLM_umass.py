import unittest, time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_umass(self):
        # filename, Y, timeoutSecs
        # fix. the ones with comments may want to be a gaussian?
        csvFilenameList = [
            # ('cgd.dat', None, 10)
            ('chdage.dat', 2, 5),
            # hangs during parse for some reason
            # ('clslowbwt.dat', 7, 5),
            ('icu.dat', 11, 5),
            ('lowbwt.dat', 1, 5),
            ('lowbwtm11.dat', 1, 5),
            # ('meexp.dat', None, 5),
            ('nhanes3.dat', 15, 5),
            # ('pbc.dat', None, 5),
            # ('pharynx.dat', None, 5),
            ('pros.dat', 1, 5),
            ('uis.dat', 8, 5),
            ]

        trial = 0
        for (csvFilename, Y, timeoutSecs) in csvFilenameList:
            csvPathname = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            print "\n" + csvPathname
            kwargs = {'xval': 0, 'case': 'NaN', 'family': 'binomial', 'link': 'familyDefault', 'Y': Y}
            start = time.time()
            glm = h2o_cmd.runGLM(csvPathname=csvPathname, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname, 'took', time.time() - start, 'seconds'
            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
