# Test R functionality for Demo
# to invoke this you need R 2.15 as of now
# R -f H2OTestDemo.R 
source("./H2O.R")

# Run expressions on covtype
h2o.importFile("covtype", paste(getwd(),"../../datasets/UCI/UCI-large/covtype/covtype.data",sep="/"))
h2o.inspect("covtype")
h2o(slice(covtype,100,100))
h2o(sum(covtype[12]))
h2o.glm(covtype, Y = 12, family=binomial)
h2o.filter(covtype, covtype[6] < mean(covtype[6]))
h2o(covtype[1] + covtype[2] * 4 + max(covtype[6]) * covtype[1] + 7 - covtype[3])
h2o(log(covtype[1]))
# add randomforest test
h2o.rf(covtype, class = "54", ntree = "10")

# Run GLM
h2o.importFile("prostate", paste(getwd(),"../smalldata/logreg/prostate.csv",sep="/"))
h2o.inspect("prostate")
h2o.glm(prostate, Y = CAPSULE, family=binomial)
