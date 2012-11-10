library('RCurl');
library('rjson');

# R <-> H2O Interop Layer
#
# This is a new version that will eventually be packaged. Comments are yet missing, will be added soon.


#is.defined <- function(x) {
#  # !is.null is ugly :)
#  return (!is.null(x))
#}

# Public functions & declarations -------------------------------------------------------------------------------------

h2o.SERVER = "localhost:54321"
h2o.VERBOSE = TRUE
h2o.MAX_GET_ROWS = 200000


h2o <- function(expr,maxRows = h2o.MAX_GET_ROWS, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  keyName = h2o.exec(expr)
  h2o.get(keyName, maxRows, forceDataFrame)
}

h2o.exec <- function(expr) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  h2o.__printIfVerbose("  Executing expression ",expr)
  res = h2o.__remoteSend(h2o.__PAGE_EXEC,Expr=expr)
  res$ResultKey
}

h2o.get <- function(keyName, maxRows = h2o.MAX_GET_ROWS, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Getting key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_GET, Key = keyName, maxRows = maxRows)
  h2o.__convertToRData(res,forceDataFrame = forceDataFrame)
}

h2o.put <- function(keyName, value) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Putting a vector of ",length(value)," to key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_PUT,Key = keyName, Value = paste0(value,collapse=" "))
  res$Key
}

h2o.inspect <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Inspecting key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_INSPECT, Key = keyName)
  result = list()
  result$key = res$key
  result$type = res$type
  result$rows = res$rows
  result$cols = res$cols
  result$rowSize = res$rowsize
  result$size = res$size
  h2o.__printIfVerbose("  key has ",res$cols," columns and ",res$rows," rows")  
  # given correct inspect JSON response, converts its columns to a dataframe
  extract <- function(from,what) {
    result = NULL
    for (i in 1:length(from)) 
      result = c(result,from[[i]][what]);
    result;    
  }
  res = res$columns;
  result$columns = data.frame(name = as.character(extract(res,"name")),
                              offset = as.numeric(extract(res,"off")),
                              type = as.character(extract(res,"type")),
                              size = as.numeric(extract(res,"size")),
                              base = as.numeric(extract(res,"base")),
                              scale = as.numeric(extract(res,"scale")),
                              min = as.numeric(extract(res,"min")),
                              max = as.numeric(extract(res,"max")),
                              badat = as.numeric(extract(res,"badat")),
                              mean = as.numeric(extract(res,"mean")),
                              var = as.numeric(extract(res,"var")))
  result
}

h2o.remove <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Removing key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_REMOVE, Key = keyName)
  res$Key
}

h2o.importUrl <- function(keyName, url, parse = TRUE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Importing url ",url," to key ",keyName)
  if (parse)
    uploadKey = url
  else
    uploadKey = keyName
  res = h2o.__remoteSend(h2o.__PAGE_IMPORT, Key = uploadKey, Url = url)
  if (parse) {
    h2o.__printIfVerbose("  parsing key ",uploadKey," to key ",keyName)
    res = h2o.__remoteSend(h2o.__PAGE_PARSE, Key = uploadKey, Key2 = keyName)    
  } 
  res$Key
}

h2o.importFile <- function(keyName, fileName, parse = TRUE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.importUrl(keyName,paste("file://",fileName,sep=""),parse = parse)
}

# expression shorthands -----------------------------------------------------------------------------------------------

h2o.slice <- function(keyName, startRow, length=-1, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  if (length == -1)
    expr = paste("slice(",keyName,",",startRow,")",sep="")
  else
    expr = paste("slice(",keyName,",",startRow,",",length,")",sep="")
  resultKey = h2o.__exec(expr)
  h2o.__get(resultKey,maxRows = h2o.MAX_GET_ROWS, forceDataFrame)
}

# Internal functions & declarations -----------------------------------------------------------------------------------

h2o.__PAGE_EXEC = "Exec.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_REMOVE = "Remove.json"
h2o.__PAGE_IMPORT = "ImportUrl.json"
h2o.__PAGE_PARSE = "Parse.json"


h2o.__printIfVerbose <- function(...) {
  if (h2o.VERBOSE == TRUE)
    cat(paste(...,"\n",sep=""))
}

h2o.__remoteSend <- function(page,...) {
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste(h2o.SERVER,page,sep="/")
  res = fromJSON(postForm(url,...))
  if (is.defined(res$Error))
    stop(paste(url," returned the following error:\n",h2o.__formatError(res$Error)))
  res    
}

h2o.__formatError <- function(error,prefix="  ") {
  result = ""
  items = strsplit(error,"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result,prefix,items[i],"\n",sep="")
  result
}

h2o.__convertToRData <- function(res,forceDataFrame=FALSE) {
  # converts the given response to an R vector or dataframe. Vector is used when there is only one column, otherwise dataframe is used. 
  if (!forceDataFrame && (length(res$columns) == 1)) {
    h2o.__printIfVerbose("  converting returned ",res$num_cols," columns and ",res$sent_rows," rows to an R vector")
    res$columns[[1]]$contents
  } else {
    h2o.__printIfVerbose("  converting returned ",res$num_cols," columns and ",res$sent_rows," rows to an R data frame")
    r = data.frame()
    rows = res$sent_rows
    for (i in 1:length(res$columns)) {
      col = res$columns[[i]]
      name = as.character(col$name)
      r[1:rows, name] <- col$contents 
    }
    r
  }
}

# h2o.rf <- function(key,ntree, depth=30,model=FALSE,gini=1,seed=42,wait=TRUE) {
#   if (model==FALSE)
#     model = paste(key,"_model",sep="")
#   H2O._printIfVerbose("  executing RF on ",key,", ",ntree," trees , maxDepth ",depth,", gini ",gini,", seed ",seed,", model key ",model)
#   res = H2O._remoteSend("RF.json",Key=key, ntree=ntree, depth=depth, gini=gini, seed=seed, modelKey=model)
#   if (H2O._isError(res)) {
#     H2O._printError(res$Error,prefix="  ")
#     NULL
#   } else {
#     H2O._printIfVerbose("    task for building ",res$ntree," from data ",res$dataKey)
#     H2O._printIfVerbose("    model key: ",res$modelKey)
#     res$modelKey
#   }
# }
# 
# h2o.rfView <- function(dataKey, modelKey) {
#   H2O._printIfVerbose("  RF model request for data ",dataKey," and model ",modelKey)
#   res = H2O._remoteSend("RFView.json",dataKey=dataKey, modelKey=modelKey)
#   if (H2O._isError(res)) {
#     H2O._printError(res$Error,prefix="  ")
#     NULL
#   } else {
#     res 
#   }  
# }

