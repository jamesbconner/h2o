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
   echo scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE0}:${REMOTE_WDIR}
   scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE0}:${REMOTE_WDIR}
  done
}

function echo_launch(){
 REMOTE_WDIR=/home/${HD_USER}/${CLOUD_NAME}
 H2O_REMOTE_CMD="'cd ${REMOTE_WDIR}; rm -fr ice_${CLOUD_NAME};java -Xmx8g -jar ${REMOTE_WDIR}/h2o.jar -name $CLOUD_NAME --ice_root=ice_${CLOUD_NAME}' &"
 echo $H2O_REMOTE_CMD;
 for NODE in ${NODE0} ${NODE1} ${NODE2} ${NODE3} ${NODE4} ${NODE5}
  do
   echo xterm -e ssh -t ${HD_USER}@${NODE} ${H2O_REMOTE_CMD} >> _run.sh
  done
}

# run
build
dist
# delete prior run script
rm ./_run.sh
echo_launch
sh ./_run.sh
