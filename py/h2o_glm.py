# cheatsheet for current GLM json return hierachy
# h2o
# key
# GLMModel
#     GLMParams
#         betaEps
#         caseVal 
#         family
#         link
#         maxIter
#         threshold
#         weight
#     LSMParams
#         penalty
#     coefficients
#         <col>
#         Intercept
#     dataset
#     isDone
#     iterations
#     time
#     validations
#         cm
#         dataset
#         dof
#         err
#         nrows
#         nullDev
#         resDev
# xval
#     is a list of things that match GLMModel?

# params on family=gaussian? No xval?
# conditional set depends on family=
# {
# "GLMParams": {
# "betaEps": 0.0001, 
# "family": "gaussian", 
# "link": "identity", 
# "maxIter": 50
# }, 
# "LSMParams": {
# "lambda": 1e-08, 
# "penalty": "L2"
# }, 

import h2o_cmd, h2o

def simpleCheckGLM(self, glm, colX, **kwargs):
    # h2o GLM will verboseprint the result and print errors. 
    # so don't have to do that
    # different when xvalidation is used? No trainingErrorDetails?
    GLMModel = glm['GLMModel']
    if 'warnings' in GLMModel:
        warnings = GLMModel['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

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
    coefficients = GLMModel['coefficients']
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
    absIntercept = abs(float(coefficients['Intercept']))
    self.assertGreater(absIntercept, 1e-18, (
        "abs. value of GLM coefficients['Intercept'] is " +
        str(absIntercept) + ", not >= 1e-18 for Intercept"
                ))

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


# compare this glm to last one. since the files are concatenations, 
# the results should be similar? 10% of first is allowed delta
def compareToFirstGlm(self, key, glm, firstglm):
    # if isinstance(firstglm[key], list):
    # in case it's not a list allready (err is a list)
    kList = list(glm[key])
    firstkList = list(firstglm[key])
    for k, firstk in zip(kList, firstkList):
        delta = .1 * float(firstk)
        msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
        self.assertAlmostEqual(float(k), float(firstk), delta=delta, msg=msg)
        self.assertGreaterEqual(float(k), 0.0, str(k) + " not >= 0.0 in current")


def simpleCheckGLMGrid(self, glmGridResult, **kwargs):
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

