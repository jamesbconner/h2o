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
TESTSRC=test-src
# and this is where the jar contents is stored relative to this file again
JAR_ROOT=lib

# additional dependencies, relative to this file, but all dependencies should be
# inside the JAR_ROOT tree so that they are packed to the jar file properly
DEPENDENCIES="${JAR_ROOT}/jama/*${SEP}${JAR_ROOT}/sigar/*${SEP}${JAR_ROOT}/apache/*${SEP}${JAR_ROOT}/junit/*${SEP}${JAR_ROOT}/gson/*${SEP}${JAR_ROOT}/asm/*"

DEFAULT_HADOOP_VERSION="1.0.0"
OUTDIR="build"
JAVAC=`which javac`
JAVAC_ARGS='-g
    -source 1.6
    -target 1.6
    -XDignore.symbol.file
    -Xlint:all
    -Xlint:-deprecation
    -Xlint:-serial
    -Xlint:-rawtypes
    -Xlint:-unchecked '
JAR=`which jar`
CLASSES="${OUTDIR}/classes"

# Clean up also /tmp content (/tmp/h2o-temp-*, /tmp/File*tmp)
# Note: /tmp is specific for Linux.
WIPE_TMP=false

# Load user-specific properties if they are exist.
# The properties can override the settings above.
LOCAL_PROPERTIES_FILE="./build.local.conf"
[ -f "$LOCAL_PROPERTIES_FILE" ] && source "$LOCAL_PROPERTIES_FILE"

# ------------------------------------------------------------------------------
# script  
# ------------------------------------------------------------------------------

function clean() {
    echo "cleaning..."
    rm -fr ${CLASSES}
    rm -fr ${JAR_ROOT}/init
    rm -fr ${JAR_ROOT}/hexbase_impl.jar
    rm -fr ${OUTDIR}
    if [ "$WIPE_TMP" = "true" ]; then
        echo " - wiping tmp..."
        rm -fr /tmp/h2o-temp-*
        rm -fr /tmp/File*tmp
    fi
    mkdir ${OUTDIR}
    mkdir ${CLASSES}
}

function build_classes() {
    echo "building classes..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    # javac, followed by horrible awk script to remove junk error messages about 'Unsafe'
    "$JAVAC" ${JAVAC_ARGS} \
        -cp "${CLASSPATH}" \
        -sourcepath "$SRC" \
        -d "$CLASSES" \
        $SRC/water/*java \
        $SRC/water/*/*java \
        $TESTSRC/test/*java
}

function build_h2o_jar() {
    local JAR_FILE="${JAR_ROOT}/hexbase_impl.jar"
    echo "creating jar file... ${JAR_FILE}"
    "$JAR" -cfm ${JAR_FILE} manifest.txt -C ${CLASSES} .
}

function build_initializer() {
    echo "building initializer..."
    local CLASSPATH="${JAR_ROOT}${SEP}${DEPENDENCIES}${SEP}${JAR_ROOT}/hadoop/${DEFAULT_HADOOP_VERSION}/*"
    "$JAVAC" -g -source 1.6 -target 1.6 -cp "${CLASSPATH}" -sourcepath "$SRC" -d "$JAR_ROOT" $SRC/init/*java

}

function build_jar() {
    JAR_TIME=`date "+%H.%M.%S-%m%d%y"`
    local JAR_FILE="${OUTDIR}/h2o.jar"
    echo "creating jar file... ${JAR_FILE}"
    "$JAR" -cfm ${JAR_FILE} manifest.txt -C ${JAR_ROOT} .
    echo "copying jar file... ${JAR_FILE} to ${OUTDIR}/h2o-${JAR_TIME}.jar"
    cp ${JAR_FILE} ${OUTDIR}/h2o-${JAR_TIME}.jar
}
function test_py() {
    echo "Running junit tests..."
    python py/junit.py
}

clean
if [ "$1" = "clean" ]; then exit 0; fi
build_classes
if [ "$1" = "compile" ]; then exit 0; fi
build_h2o_jar
build_initializer
build_jar
if [ "$1" = "build" ]; then exit 0; fi
test_py
