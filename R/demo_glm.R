
demo_glm <-function(key="prostate",file="../smalldata/logreg/prostate.csv",Y="CAPSULE",family="binomial") {
  #print(fromJSON(getURL("127.0.0.1:54321/Cloud.json")))
  # Load the file
  library('RCurl')
  library('rjson')
  getURL(paste("127.0.0.1:54321/PutFile.json?Key=",key,".csv&File=",file,sep=""))
  getURL(paste("127.0.0.1:54321/Parse.json?Key=____",key,".csv&Key2=____",key,".hex",sep=""))
  print(fromJSON(getURL(paste(sep="","127.0.0.1:54321/GLM.json?Key=____",key,".hex&Y=",Y,"&family=",family))))
  return (0)
}
