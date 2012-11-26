import os, json, unittest, time, shutil, sys
import h2o, h2o_cmd

# Test of glm comparing result against R-implementation
# Tested on prostate.csv short (< 1M) and long (multiple chunks)
# kbn. just updated the parseFile and runGLMonly to match the 
# higher level api now in other tests.
class GLMTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def process_dataset(self,key,Y, e_coefs, e_ndev, e_rdev, e_aic, **kwargs):
        glm = h2o_cmd.runGLMOnly(parseKey = key, Y = 'CAPSULE', timeoutSecs=10, **kwargs)
        coefs = glm['coefficients']
        errors = ['%s: %f != %f' % (x,e_coefs[x],coefs[x]) for x in coefs if abs(float(coefs[x]) - e_coefs[x]) > 1e-2]
#        if(abs(round(float(glm['trainingSetValidation']['NullDeviance']),1) - e_ndev) > 1e-1): errors.append('NullDeviance: %f != %s' % (e_ndev,glm['trainingSetValidation']['NullDeviance']))
#        if(abs(round(float(glm['trainingSetValidation']['ResidualDeviance']),1) - e_rdev) > 1e-1): errors.append('ResidualDeviance %f != %s' % (e_rdev,glm['trainingSetValidation']['ResidualDeviance']))
#        if(abs(round(float(glm['trainingSetValidation']['AIC']),1) - e_aic) > 1e-1): errors.append('AIC: %f != %s' % (e_aic,glm['trainingSetValidation']['AIC']))
        return errors
    
    def test_prostate_gaussian(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname="../smalldata/logreg/prostate.csv")
        # R results
        gaussian_coefficients = {"Intercept":-0.7514884, "ID":0.0002837,"AGE":-0.0018095,"RACE":-0.0899998, "DPROS":0.0915640,"DCAPS":0.1087697,"PSA":0.0035715, "VOL":-0.0020102,"GLEASON":0.1514025}
        gaussian_nd  = 90.36
        gaussian_rd  = 64.85
        gaussian_aic = 426.2
        errors = self.process_dataset(key, 'CAPSULE', gaussian_coefficients, gaussian_nd, gaussian_rd, gaussian_aic, family = 'gaussian')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname = "../smalldata/logreg/prostate_long.csv.gz")
        errors = self.process_dataset(key, 'CAPSULE', gaussian_coefficients, gaussian_nd, gaussian_rd, gaussian_aic, family = 'gaussian')
        if errors:
            self.fail(str(errors))

    def test_prostate_binomial(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname = "../smalldata/logreg/prostate.csv")
        # R results
        binomial_coefficients = {"Intercept":-7.774101, "ID":0.001628,"AGE":-0.011777,"RACE":-0.681977, "DPROS":0.547378,"DCAPS":0.543187,"PSA":0.027015, "VOL":-0.011149,"GLEASON":1.006152}
        binomial_nd  = 506.6
        binomial_rd  = 375.5
        binomial_aic = 393.5
        errors = self.process_dataset(key, 'CAPSULE', binomial_coefficients, binomial_nd, binomial_rd, binomial_aic, family = 'binomial')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname = "../smalldata/logreg/prostate_long.csv.gz")
        errors = self.process_dataset(key, 'CAPSULE', binomial_coefficients, binomial_nd, binomial_rd, binomial_aic, family = 'binomial')
        if errors:
            h2selfo.fail(str(errors))

    def test_prostate_poisson(self):
        errors = []
        # First try on small data (1 chunk)
        key = h2o_cmd.parseFile(csvPathname = "../smalldata/logreg/prostate.csv")
        # R results
        poisson_coefficients = {"Intercept":-3.981788, "ID":0.000529,"AGE":-0.005709,"RACE":-0.165628, "DPROS":0.227885,"DCAPS":0.075427,"PSA":0.002904, "VOL":-0.007350,"GLEASON":0.438839}
        poisson_nd  = 275.5
        poisson_rd  = 214.7
        poisson_aic = 534.7
        errors = self.process_dataset(key, 'CAPSULE', poisson_coefficients, poisson_nd, poisson_rd, poisson_aic, family = 'poisson')
        if errors:
            self.fail(str(errors))
        # Now try on larger data (replicated), will be chunked this time, should produce same results
        key = h2o_cmd.parseFile(csvPathname = "../smalldata/logreg/prostate_long.csv.gz")
        errors = self.process_dataset(key, 'CAPSULE', poisson_coefficients, poisson_nd, poisson_rd, poisson_aic, family = 'poisson')
        if errors:
            self.fail(str(errors))



if __name__ == '__main__':
    h2o.unit_main()
