library('RCurl');
library('rjson');

# H2O <-> R Interop Layer
#
# We use the S3 OOP system in R for its simplicity and ease of use. Also I have learned that these are most widely used.
#
# An example session will thus be:
#
# a = H2O.Connect()
# exec(a,"4+5")
#
# I know it is not the h2o(expr) that was suggested, but this is how people are used to do things in R (I was told). 
# Please let me know any comments on the matter:)

# ---------------------------------------------------------------------------------------------------------------------

H2O.Connect <- function(server="localhost:54321", verbose=TRUE) {
  # Creates the connection to the H2O server, checks its capabilities and returns the connection object. Connects to
  # the server and obtains its cloud information. If verbose mode is enabled (by default) also prints the information
  # as it goes.
  x <- list()
  x$server = server
  x$verbose = verbose
  class(x) <- "H2O"
  H2O._printIfVerbose(x,"Connecting to server ",server)
  x$cloud <- H2O._remoteSend(x,"Cloud.json",Client="R Interop 0.1")
  H2O._printConnection(x)
  H2O._printIfVerbose(x,"  size ",x$cloud$cloud_size)
  x
}

# ---------------------------------------------------------------------------------------------------------------------

exec.H2O <- function(x, expr) {
  # Executes the given expression of the H2O server specified
  H2O._printConnection(x)
  H2O._printIfVerbose(x,"  sending expression ",expr)
  res = H2O._remoteSend(x,"Exec.json",Expr=expr)
  if (H2O._isError(res)) {
    H2O._printError(x,res$Error,prefix="  ")
    NULL
  } else {
    inspect(x,res$Result)
  }
}

inspect.H2O <- function(x, key) {
  # Returns the inspected key
  H2O._printConnection(x)
  H2O._printIfVerbose(x,"  inspecting key ",key)
  res = H2O._remoteSend(x,"Inspect.json",Key=key)
  if (H2O._isError(res)) {
    H2O._printError(x,res$Error,prefix="  ")
    NULL
  } else {
    H2O._inspectToDataFrame(res)
  }
}

put.H2O <- function(x, key, value) {
  # Puts the given vector to the H2O cloud under the given key name. Returns the key name itself.
  H2O._printConnection(x)
  H2O._printIfVerbose(x,"  put of vector (size ",length(value),")")
  res = H2O._remoteSend(x,"PutVector.json",Key=key,Value=paste0(value,collapse=" "))
  if (H2O._isError(res)) {
    H2O._printError(x,res$Error,prefix="  ")
    NULL
  } else {
    res$Key
  }
}

# ---------------------------------------------------------------------------------------------------------------------

is.defined <- function(x) {
  # !is.null is ugly :)
  return (!is.null(x))
}

H2O._printIfVerbose <- function(x,...,prefix="") {
  # If given connection is set to verbose, prints the given message
  if (x$verbose)
    cat(paste(...,"\n",sep=""))
}

H2O._remoteSend <- function(x,page,...) {
  # Sends given page request to the specified server and return its result
  url <- paste(x$server, page, sep = "/")
  fromJSON(postForm(url,...))
}

H2O._printConnection <- function(x) {
  # prints the basic connection information (cloud name and connected node)
  H2O._printIfVerbose(x,"Cloud ",x$cloud$cloud_name,", node ",x$cloud$node_name,":")
}

H2O._printError <- function(x,error,prefix="") {
  # prints given error (splits lines if necessary)
  items = strsplit(errString,"\n")[[1]];
  for (i in 1:length(items))
    cat(paste(prefix,items[i],"\n"))
}

H2O._isError <- function(response) {
  # Returns true if the JSON response was is an error report
  (is.defined(response$Error))
}

H2O._inspectToDataFrame <- function(response) {
  # given correct inspect JSON response, converts it to a dataframe
  extract <- function(from,what) {
    result = NULL
    for (i in 1:length(from)) 
      result = c(result,from[[i]][what]);
    result;    
  }
  res = response$columns;
  data.frame(name = as.character(extract(res,"name")),
             offset = as.numeric(extract(res,"off")),
             type = as.character(extract(res,"type")),
             size = as.numeric(extract(res,"size")),
             base = as.numeric(extract(res,"base")),
             scale = as.numeric(extract(res,"scale")),
             min = as.numeric(extract(res,"min")),
             max = as.numeric(extract(res,"max")),
             badat = as.numeric(extract(res,"badat")),
             mean = as.numeric(extract(res,"mean")),
             var = as.numeric(extract(res,"var")));
}

# ---------------------------------------------------------------------------------------------------------------------
# S3 functions -- methods layer

exec <- function(this, ...) {
  UseMethod("exec",this)
}

inspect <- function(this, ...) {
  UseMethod("inspect",this)
}

put <- function(this, ...) {
  UseMethod("put",this)
}
