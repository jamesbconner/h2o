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
def simpleCheckGLM(glm,colX):
    # h2o GLM will verboseprint the result and print errors. 
    # so don't have to do that
    # different when xvalidation is used? No trainingErrorDetails?
    GLMModel = glm['GLMModel']
    print "GLM time", GLMModel['time']

    GLMParams = GLMModel["GLMParams"]
    family = GLMParams["family"]

    # no trainingErrorDetails if poisson? 
    # pop the first validation from the list
    validationsList = GLMModel['validations']
    validations = validationsList.pop()
    print "\nGLMModel/validations/err:", validations['err']

    if (family=="poisson"):
        pass
    else:
        # no cm in poisson?
        cmList = validations['cm']

        xvalList = glm['xval']
        xval = xvalList.pop()
        # FIX! why is this returned as a list? no reason?
        validationsList = xval['validations']
        validations = validationsList.pop()
        print "\nxval/../validations/err:", validations['err']

    # it's a dictionary!
    coefficients = GLMModel['coefficients']
    print "\ncoefficients:", coefficients
    # pick out the coefficent for the column we enabled.
    absXCoeff = abs(float(coefficients[str(colX)]))
    # intercept is buried in there too
    absIntercept = abs(float(coefficients['Intercept']))

    if (1==0):
        self.assertGreater(absXCoeff, 0.000001, (
            "abs. value of GLM coefficients['" + str(colX) + "'] is " +
            str(absXCoeff) + ", not >= 0.000001 for X=" + str(colX)
            ))

        self.assertGreater(absIntercept, 0.000001, (
            "abs. value of GLM coefficients['Intercept'] is " +
            str(absIntercept) + ", not >= 0.000001 for X=" + str(colX)
                    ))
