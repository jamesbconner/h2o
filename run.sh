#! /bin/sh

H2O_HOME=.
cd $H2O_HOME
NODE0=192.168.1.150
NODE1=192.168.1.151
NODE2=192.168.1.152
NODE3=192.168.1.153
NODE4=192.168.1.154

CLOUD_NAME=$USER
HD_USER=hduser
#is_hdfs()
HDFS_CONF="-hdfs hdfs://192.168.1.151 -hdfs_version cdh4 -hdfs_root /datasets"
#delete_ice
ICE_DIR_NAME=ice_${CLOUD_NAME}
DELETE_ICE="rm -fr $ICE_DIR_NAME"
JAR_TIME=`date "+%H.%M.%S-%m%d%y"`
# compile
function build() {
 # build 
 ./build.sh
 #ant clean; ant
}


###
# Ensure that ssh doesnot ask-a password off of you.
# For that make
# ssh-keygen
# On destination host:
# add ~/.ssh/ida_pub ~/.ssh/authorized_keys
## likeso
# cat .ssh/id_rsa.pub | ssh $NODE1 'cat >> .ssh/authorized_keys'
# cat .ssh/id_rsa.pub | ssh $NODE2 'cat >> .ssh/authorized_keys'
#
# see:
# http://www.linuxproblem.org/art_9.html
###

#copy distro

function dist(){
REMOTE_WDIR=/home/${HD_USER}/${CLOUD_NAME}
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} ${NODE4} ${NODE5}
  do
   #ssh -t ${HD_USER}@${NODE} 'mkdir ${REMOTE_WDIR}'
   echo scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE}:${REMOTE_WDIR}/h2o-${JAR_TIME}.jar
   scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE}:${REMOTE_WDIR}/h2o-${JAR_TIME}.jar
  done
}
function shutdown(){
 DEFAULT_PORT=54321
 SHUTDOWN_CMD="curl http://${NODE0}:${DEFAULT_PORT}/Shutdown"
 echo $SHUTDOWN_CMD
 $SHUTDOWN_CMD
}
function echo_launch(){
 REMOTE_WDIR=/home/${HD_USER}/${CLOUD_NAME}
 H2O_REMOTE_CMD="'cd ${REMOTE_WDIR}; ${DELETE_ICE};java -Xmx8g -jar ${REMOTE_WDIR}/h2o-${JAR_TIME}.jar -name $CLOUD_NAME --ice_root=${ICE_DIR_NAME}' &"
 echo $H2O_REMOTE_CMD;
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} ${NODE4} ${NODE5}
  do
   echo xterm -e ssh -t ${HD_USER}@${NODE} ${H2O_REMOTE_CMD} >> _run.sh
  done
}

# run
#build
dist
shutdown
# delete prior run script
rm ./_run.sh
echo_launch
sh ./_run.sh
