library('RCurl')
library('rjson')

demo_glm <- function(host_port="127.0.0.1:54321",key="prostate",file="../smalldata/logreg/prostate.csv",Y="CAPSULE",family="binomial") {
  print(fromJSON(getURL(paste(host_port,"/Cloud.json",sep=""))))
  # Load the file
  put_url=paste(host_port,'/PutFile.json?Key=',key,'.csv&File=',file,sep='')
  getURL(put_url)
  getURL(paste(host_port,'/Parse.json?Key=____',key,'.csv&Key2=____',key,'.hex',sep=''))
  print(fromJSON(getURL(paste(host_port,'/GLM.json?Key=____',key, '.hex&Y=', Y, '&family=',family,sep=''))))
  return (0)
}
