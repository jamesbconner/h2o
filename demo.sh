#! /bin/sh

H2O_HOME=.
cd $H2O_HOME
NODE0=192.168.1.150
NODE1=192.168.1.151
NODE2=192.168.1.152
NODE3=192.168.1.153
NODE4=192.168.1.154
NODE5=192.168.1.155
NODE6=192.168.1.156
NODE7=192.168.1.157

CLOUD_NAME=$USER
HD_USER=hduser
#is_hdfs()
HDFS_CONF="-hdfs hdfs://192.168.1.151 -hdfs_version cdh4 -hdfs_root /datasets"
#delete_ice
ICE_DIR_NAME=ice_${CLOUD_NAME}
DELETE_ICE="rm -fr ${ICE_DIR_NAME}*"
JAR_TIME=`date "+%H.%M.%S-%m%d%y"`
# compile
function build() {
 # build 
 ./build.sh
# ant clean; ant
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
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} ${NODE4} ${NODE5} ${NODE6} ${NODE7} 
  do
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
function echo_prep_launch(){
 REMOTE_WDIR=/home/${HD_USER}/${CLOUD_NAME}
 for i in 1 
 do
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} 
  do
   PREP_REMOTE_CMD="'killall java; mkdir ${REMOTE_WDIR}_${i}; cd ${REMOTE_WDIR}_${i}; ${DELETE_ICE}; exit;'"
   echo xterm -e ssh -t ${HD_USER}@${NODE} ${PREP_REMOTE_CMD} >> _run.sh
  done
 done
}
function echo_launch(){
 REMOTE_WDIR=/home/${HD_USER}/${CLOUD_NAME}
 #H2O_REMOTE_CMD="'cd ${REMOTE_WDIR}; ${DELETE_ICE};java -Xmx4g -jar ${REMOTE_WDIR}/h2o-${JAR_TIME}.jar -name $CLOUD_NAME --ice_root=${ICE_DIR_NAME} --nosigar' &"
 echo $H2O_REMOTE_CMD;
 for i in 1 
 do
 #for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} ${NODE4} ${NODE5} ${NODE6} ${NODE7}
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} 
  do
   H2O_REMOTE_CMD="'cd  ${REMOTE_WDIR}_${i};/home/hduser/jdk1.6.0_31/bin/java -Xmx10g -jar ${REMOTE_WDIR}/h2o-${JAR_TIME}.jar -name $CLOUD_NAME --ice_root=${ICE_DIR_NAME} ${HDFS_CONF} --nosigar;sleep 10' &"
   echo xterm -title ${NODE} -e ssh -t ${HD_USER}@${NODE} ${H2O_REMOTE_CMD} >> _run.sh
  done
   echo "sleep 10" >> _run.sh
 done
}

# run
build
dist
shutdown
sleep 5
# delete prior run script
rm ./_run.sh
echo_prep_launch
echo_launch
sh ./_run.sh
