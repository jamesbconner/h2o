import h2o_cmd, h2o
import re

def simpleCheckGLM(self, glm, colX, allowFailWarning=False, **kwargs):
    # h2o GLM will verboseprint the result and print errors. 
    # so don't have to do that
    # different when xvalidation is used? No trainingErrorDetails?
    GLMModel = glm['GLMModel']
    warnings = None
    if 'warnings' in GLMModel:
        warnings = GLMModel['warnings']
        # catch the 'Failed to converge" for now
        x = re.compile("[Ff]ailed")
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w) and not allowFailWarning: raise Exception(w)

    print "GLM time", GLMModel['time']

    # FIX! don't get GLMParams if it can't solve?
    GLMParams = GLMModel["GLMParams"]
    family = GLMParams["family"]

    iterations = GLMModel['iterations']
    print "\nGLMModel/iterations:", iterations

    # pop the first validation from the list
    validationsList = GLMModel['validations']
    # don't want to modify validationsList in case someone else looks at it
    validations = validationsList[0]
    print "\nGLMModel/validations/err:", validations['err']

    if (not family in kwargs) or kwargs['family']=='poisson' or kwargs['family']=="gaussian":
        # FIX! xval not in gaussian or poisson?
        pass
    else:
        if ('xval' in kwargs):
            # no cm in poisson?
            cmList = validations['cm']

            xvalList = glm['xval']
            xval = xvalList[0]
            # FIX! why is this returned as a list? no reason?
            validationsList = xval['validations']
            validations = validationsList[0]
            print "\nxval/../validations/err:", validations['err']

    # it's a dictionary!
    # get a copy, so we don't destroy the original when we pop the intercept
    coefficients = GLMModel['coefficients'].copy()
    # get the intercept out of there into it's own dictionary
    intercept = coefficients.pop('Intercept', None)

    print "\ncoefficients:", coefficients
    # pick out the coefficent for the column we enabled.

    # FIX! temporary hack to deal with disappaering/renaming columns in GLM
    if colX is not None:
        absXCoeff = abs(float(coefficients[str(colX)]))
        self.assertGreater(absXCoeff, 1e-18, (
            "abs. value of GLM coefficients['" + str(colX) + "'] is " +
            str(absXCoeff) + ", not >= 1e-18 for X=" + str(colX)
            ))

    # intercept is buried in there too
    absIntercept = abs(float(intercept))
    self.assertGreater(absIntercept, 1e-18, (
        "abs. value of GLM coefficients['Intercept'] is " +
        str(absIntercept) + ", not >= 1e-18 for Intercept"
                ))


    maxCoeff = max(coefficients, key=coefficients.get)
    print "Largest coefficient value:", maxCoeff, coefficients[maxCoeff]
    minCoeff = min(coefficients, key=coefficients.get)
    print "Smallest coefficient value:", minCoeff, coefficients[minCoeff]

    # many of the GLM tests aren't single column though.
    # quick and dirty check: if all the coefficients are zero, 
    # something is broken
    # intercept is in there too, but this will get it okay
    # just sum the abs value  up..look for greater than 0

    s = 0.0
    for c in coefficients:
        v = coefficients[c]
        s += abs(float(v))
        self.assertGreater(s, 1e-18, (
            "sum of abs. value of GLM coefficients/intercept is " + str(s) + ", not >= 1e-18"
            ))

    
    return warnings


# compare this glm to last one. since the files are concatenations, 
# the results should be similar? 10% of first is allowed delta
def compareToFirstGlm(self, key, glm, firstglm):
    # if isinstance(firstglm[key], list):
    # in case it's not a list allready (err is a list)
    h2o.verboseprint("compareToFirstGlm key:", key)
    h2o.verboseprint("compareToFirstGlm glm[key]:", glm[key])
    # key could be a list or not. if a list, don't want to create list of that list
    # so use extend on an empty list. covers all cases?
    if type(glm[key]) is list:
        kList  = glm[key]
        firstkList = firstglm[key]
    elif type(glm[key]) is dict:
        raise Exception("compareToFirstGLm: Not expecting dict for " + key)
    else:
        kList  = [glm[key]]
        firstkList = [firstglm[key]]

    for k, firstk in zip(kList, firstkList):
        delta = .1 * float(firstk)
        msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
        self.assertAlmostEqual(float(k), float(firstk), delta=delta, msg=msg)
        self.assertGreaterEqual(float(k), 0.0, str(k) + " not >= 0.0 in current")


def simpleCheckGLMGrid(self, glmGridResult, colX=None, allowFailWarning=False, **kwargs):
    destination_key = glmGridResult['destination_key']
    inspectGG = h2o_cmd.runInspect(None, destination_key)
    print "Inspect of destination_key", destination_key,":\n", h2o.dump_json(inspectGG)

    # FIX! currently this is all unparsed!
    type = inspectGG['type']
    if 'unparsed' in type:
        print "Warning: GLM Grid result destination_key is unparsed, can't interpret. Ignoring for now"
        print "Run with -b arg to look at the browser output, for minimal checking of result"

    cols = inspectGG['cols']
    response = inspectGG['response'] # dict
    rows = inspectGG['rows']
    value_size_bytes = inspectGG['value_size_bytes']

# models entries look like this:
#     {
#       "alpha": 1.0, 
#       "area_under_curve": 0.16666666666666669, 
#       "best_threshold": 0.0, 
#       "error_0": 1.0, 
#       "error_1": 0.0, 
#       "key": "__GLMModel_8b0fc26c-3a9c-4c4b-8cf6-240cc5b60508", 
#       "lambda_1": 0.009999999999999998, 
#       "lambda_2": 0.9999999999999999, 
#       "rho": 1e-06
#     }, 
    model0 = glmGridResult['models'][0]
    alpha = model0['alpha']
    area_under_curve = model0['area_under_curve']
    error_0 = model0['error_0']
    error_1 = model0['error_1']
    key = model0['key']
    print "best GLM model key:", key

    lambda_1 = model0['lambda_1']
    lambda_2 = model0['lambda_2']
    rho = model0['rho']

    # now indirect to the GLM result/model that's first in the list (best)
    inspectGLM = h2o_cmd.runInspect(None, key)
    print "GLMGrid inspectGLM:", h2o.dump_json(inspectGLM)
    simpleCheckGLM(self, inspectGLM, colX, allowFailWarning=allowFailWarning, **kwargs)

