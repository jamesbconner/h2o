library('rjson');
# Class representing .hex dataset stored in cloud
setClass(
	Class = "h2o.data.frame",
	representation = representation(
		key = "character",
		ncolumns = 'integer',
		nrows = 'integer',
		max = 'numeric', #vector of max column values
		min = 'numeric', # vector of min column values,
		colnames = 'character'		
	)
)