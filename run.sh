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
ant clean; ant


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

scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE0}:/home/$HD_USER/${USER}
scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE1}:/home/$HD_USER/${USER}
scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE2}:/home/$HD_USER/${USER}
scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE3}:/home/$HD_USER/${USER}
scp ${H2O_HOME}/build/h2o.jar ${HD_USER}@${NODE4}:/home/$HD_USER/${USER}

#localhost
xterm -e java -jar build/h2o.jar -name $USER &
#
xterm -e ssh -t ${HD_USER}@$NODE0 'rm -fr ice;java -Xmx8g -jar /home/${HD_USER}/${USER}/h2o.jar -name $USER --ice_root=ice' &
xterm -e ssh -t ${HD_USER}@$NODE1 'rm -fr ice;java -Xmx8g -jar /home/${HD_USER}/${USER}/h2o.jar -name $USER --ice_root=ice' &
xterm -e ssh -t ${HD_USER}@$NODE2 'rm -fr ice;java -Xmx8g -jar /home/${HD_USER}/${USER}/h2o.jar -name $USER --ice_root=ice' &
xterm -e ssh -t ${HD_USER}@$NODE3 'rm -fr ice;java -Xmx8g -jar /home/${HD_USER}/${USER}/h2o.jar -name $USER --ice_root=ice' &
xterm -e ssh -t ${HD_USER}@$NODE4 'rm -fr ice;java -Xmx8g -jar /home/${HD_USER}/${USER}/h2o.jar -name $USER --ice_root=ice' &
