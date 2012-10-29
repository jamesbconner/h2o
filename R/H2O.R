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

H2O.SERVER = "localhost:54321"
H2O.VERBOSE = TRUE
H2O.MAX_RESPONSE_ITEMS = 200000

h2o <- function(expr) {
  # Executes the given expression on H2O server and returns the result as R variable. It may error if the result is
  # too big as it is still work in progress.
  res = h2o.exec(expr)
  if (is.defined(res))
    h2o.get(res)
  else
    NULL
}

h2o.connect <- function(server = H2O.SERVER, verbose = H2O.VERBOSE) {
  # Creates the connection to the H2O server, checks its capabilities and returns the connection object. Connects to
  # the server and obtains its cloud information. If verbose mode is enabled (by default) also prints the information
  # as it goes.
  H2O.SERVER = server
  H2O.VERBOSE = verbose
  x <- list()
  x$server = server
  x$verbose = verbose
  H2O._printIfVerbose("Connecting to server ",server)
  x$cloud <- H2O._remoteSend("Cloud.json",Client="R Interop 0.1")
  H2O._printIfVerbose("  size ",x$cloud$cloud_size)
  x
}

h2o.inspect <- function(key) {
  # Returns the inspected key
  H2O._printIfVerbose("  inspecting key ",key)
  res = H2O._remoteSend("Inspect.json",Key=key)
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    H2O._inspectToDataFrame(res)
  }
}

h2o.exec <- function(expr) {
  H2O._printIfVerbose("  sending expression ",expr)
  res = H2O._remoteSend("Exec.json",Expr=expr)
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    res$Result
  }
}

h2o.put <- function(key, value) {
  # Puts the given vector to the H2O cloud under the given key name. Returns the key name itself. Current limit on the
  # vector is that it must be only a single column vector and have no more than 200000 items. 
  H2O._printIfVerbose("  put of vector (size ",length(value),")")
  res = H2O._remoteSend("PutVector.json",Key=key,Value=paste0(value,collapse=" "))
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    res$Key
  }
}

h2o.import <- function(key, file, hex=TRUE) {
  # imports url to the server. This is probably only worth our debugging. Probably. 
  if (hex) {
    uploadKey = file
  } else {
    uploadKey = key
  }
  H2O._printIfVerbose("  put file ",file," to key ",uploadKey)
  res = H2O._remoteSend("ImportUrl.json",Url=file)
  if (!H2O._isError(res) && hex) {
    H2O._printIfVerbose("  parsing key ",res$Key," to key ",key)
    res = H2O._remoteSend("Parse.json",Key=res$Key, Key2=key)
  }
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    res$Key
  }
} 

h2o.remove <- function(key) {
  # deletes the given UKV key. 
  H2O._printIfVerbose("  removing key ",key)
  res = H2O._remoteSend("Remove.json",Key=key)
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    res$Key
  }
}

h2o.rf <- function(key,numTrees,maxDepth=30,model="model",gini=1,seed=42) {
  H2O._printIfVerbose("  executing RF on ",key,", ",numTrees," trees , maxDepth ",maxDepth,", gini ",gini,", seed ",seed,", model key ",model)
  res = H2O._remoteSend("RF.json",Key=key, ntree=numTrees, depth=maxDepth, gini=gini, seed=seed, modelKey=model)
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    res
  }
  
}

h2o.get <- function(key, max=H2O.MAX_RESPONSE_ITEMS) {
  # returns the given key from H2O. DataFrames of multiple columns are supported as long as their number of elements,
  # that is columns * rows is smaller than 200000.
  H2O._printIfVerbose("  get of vector/dataframe (key ",key,")")
  res = H2O._remoteSend("GetVector.json",Key=key,MaxItems=max)
  if (H2O._isError(res)) {
    H2O._printError(res$Error,prefix="  ")
    NULL
  } else {
    H2O._printIfVerbose("    returned data frame of ",res$num_cols," column(s) and ",res$num_rows," row(s), first ",res$sent_rows," row(s) returned")
    if (length(res$columns) == 1) {
      H2O._printIfVerbose("    converting to single column vector")
      lapply(strsplit(res$columns[[1]]$contents,split=" "),as.numeric)[[1]]
    } else {
      r = data.frame()
      rows = res$sent_rows
      for (i in 1:length(res$columns)) {
        col = res$columns[[i]]
        name = as.character(col$name)
        x = lapply(strsplit(col$contents, split=" "),as.numeric)[[1]]
        r[1:rows, name] <- x
      }
      r
    }
  }
}

# ---------------------------------------------------------------------------------------------------------------------

is.defined <- function(x) {
  # !is.null is ugly :)
  return (!is.null(x))
}

H2O._printIfVerbose <- function(...,prefix="") {
  # If given connection is set to verbose, prints the given message
  if (H2O.VERBOSE)
    cat(paste(...,"\n",sep=""))
}

H2O._remoteSend <- function(page,...) {
  # Sends given page request to the specified server and return its result
  url <- paste(H2O.SERVER, page, sep = "/")
  fromJSON(postForm(url,...))
}

H2O._printError <- function(error,prefix="") {
  # prints given error (splits lines if necessary)
  items = strsplit(error,"\n")[[1]];
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

