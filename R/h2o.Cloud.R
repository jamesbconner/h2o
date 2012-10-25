library('RCurl');
library('rjson');
library('methods');
source('h2o.data.frame.R');

# Class representing the cloud
setClass("h2o.Cloud", representation (url = "character"),prototype=prototype(url="localhost:54321"));

setGeneric("haha",function(cloud){standardGeneric("haha")});

setMethod("haha",signature="h2o.Cloud",function(cloud){
	print("haha");
});


setGeneric("uploadFile",function(cloud,file){standardGeneric("uploadFile")});

setMethod("uploadFile", c(cloud="h2o.Cloud",file="character"),function(cloud,file){
	print(paste("uploading file",file, "to cloud",cloud@url));
	url <- paste(cloud@url,"PutFile.json",sep="/");
	res <- fromJSON(postForm(url,File=file));
	url <- paste(cloud@url,"Parse.json",sep="/");
	res <- fromJSON(postForm(url,Key=res$key));	
	new('h2o.data.frame', key = res$key);
});

parseX <- function(formula){
	if(length(formula) == 1)
		formula
	else if(formula[[1]] == '+') 
		paste(parseX(formula[[2]]),parseX(formula[[3]]),sep=',')
	else 
		stop("unsuported formula, only expression of type Y ~ X1 + X3 + ... + Xn are allowed");
}

setGeneric("h2o.glm",function(cloud,formula, data, family){standardGeneric("h2o.glm")});

setMethod("h2o.glm", c(cloud="h2o.Cloud",formula = "formula", data="h2o.data.frame", family="character"),function(cloud,formula,data, family = "gaussian"){
	url <- paste(cloud@url,"GLM.json",sep="/");
	if(formula[[1]] != '~') stop(paste("Unexpected formula: '",formula,"'"));
	Y <- formula[[2]];
	X <- parseX(formula[[3]]);
	res <- fromJSON(postForm(url,Key=data@key,X = X,Y = Y, family = family));
});



