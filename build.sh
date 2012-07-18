#! /bin/sh
# determine the correct separator of multiple paths
if [ `uname` = "Darwin" ]
then 
    UNAMECMD="uname"
else 
    UNAMECMD="uname -o"
fi

if [ `$UNAMECMD` = "Cygwin" ]
then
    SEP=";"
else
    SEP=":"
fi
# ------------------------------------------------------------------------------
# basic build properties
# ------------------------------------------------------------------------------
# This is where the source files (java) are relative to the path of this file
SRC=src
# and this is where the jar contents is stored relative to this file again
JAR_ROOT=jar 

# additional dependencies, relative to this file, but all dependencies should be
# inside the JAR_ROOT tree so that they are packed to the jar file properly
DEPENDENCIES="${JAR_ROOT}/lib/sigar/*${SEP}${JAR_ROOT}/lib/apache/*"

DEFAULT_HADOOP_VERSION="1.0.0"
OUTDIR="build"
JAVAC=`which javac`
JAR=`which jar`
CLASSES="${OUTDIR}/classes"

# ------------------------------------------------------------------------------
# script  
# ------------------------------------------------------------------------------

function clean() {
    rm -fr ${CLASSES}
    rm -fr ${JAR_ROOT}/init
    rm -fr ${JAR_ROOT}/hexbase_impl.jar
    mkdir ${OUTDIR}
    mkdir ${CLASSES}
}

function build_classes() {
    echo "building classes..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    # javac, followed by horrible awk script to remove junk error messages about 'Unsafe'
    "$JAVAC" -g -source 1.6 -target 1.6 -cp "${CLASSPATH}" -sourcepath "$SRC" -d "$CLASSES" $SRC/water/*java  $SRC/water/*/*java |& awk '{if( $0 !~ "proprietary" ) {print $0} else {getline;getline;}}'
}

function build_h2o_jar() {
    echo "creating jar file..."
    local JAR_FILE="${JAR_ROOT}/hexbase_impl.jar"
    "$JAR" -cfm ${JAR_FILE} manifest.txt -C ${CLASSES} .
}

function build_initializer() {
    echo "building initializer..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    "$JAVAC" -source 1.6 -target 1.6 -cp "${CLASSPATH}" -sourcepath "$SRC" -d "$JAR_ROOT" $SRC/init/*java

}

function build_jar() {
    echo "creating jar file..."
    local JAR_FILE="${OUTDIR}/h2o.jar"
    "$JAR" -cfm ${JAR_FILE} manifest.txt -C ${JAR_ROOT} .
}

function build_example() {
    echo "creating example ${1}"
    rm examples/src/*.class
    rm examples/src/${1}.jar
    local CPATH="${CLASSES}${SEP}examples/src/${SEP}${OUTDIR}/h2o.jar"
    "$JAVAC" -source 1.6 -target 1.6 -cp $CPATH examples/src/${1}.java
    cd examples/src
    "$JAR" cvf ${1}.jar -C . *class
    cd ../..
}

clean
build_classes
build_h2o_jar
build_initializer
build_jar
build_example "DLR"
build_example "LR"
build_example "H2O_WordCount1"
build_example "HelloWorld"
build_example "Average"
