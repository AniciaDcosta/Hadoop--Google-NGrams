#!/bin/bash -l

# this script will compile your hadoop program and then submit 
# it via generic.pbs to the pbs queue 

# you should make runit-generic.sh executable and then run
# from the command line as follows : 
# 
# chmod u+x runit-generic.sh
# ./runit-generic.sh
# 
# monitor your job via the qstat command
# qstat -u $USER

# once your job is finished, check the files generic.e 
# and generic.o for error and output info
#
# ------------------------------
# remove old .e and .o files

rm -fr generic.o
rm -fr generic.e

# assumes that .java .jar and class will have the same name
# as in CLASSNAME.java CLASSNAME.jar and CLASSNAME
# also assumes that .java file is in the same directory as 
# this script

# *** you will need to change this for each assignment ***

CLASSNAME=GoogleNgramsMain

# classes needed for hadoop - shouldn't usually need to change
# unless you are using some specialized library functions 

HADOOP_VERSION=1.0.3
CLASSPATH="/soft/hadoop/$HADOOP_VERSION/hadoop-core-$HADOOP_VERSION.jar:/soft/hadoop/$HADOOP_VERSION/lib/commons-cli-1.2.jar"

# you shouldn't normally need to modify anything below this line, assuming
# that you are following the suggested naming conventions  
# ---------------------------------------------------

# load up hadoop, mainly to set environment variables

module load hadoop

# check what hadoop variables get set
env | grep HADOOP

# delete old class directory, and create directory for classes 

rm -fr $CLASSNAME
mkdir $CLASSNAME

# compile

javac -classpath $CLASSPATH -d $CLASSNAME $CLASSNAME.java NgramMapper.java NgramReducer.java NgramCountMapper.java NgramCountReducer.java NgramMapper3.java FinalReducer.java
# delete old jar file and build the new one

rm -fr $CLASSNAME.jar

jar -cvf $CLASSNAME.jar -C $CLASSNAME/ .

# submit to the pbs queue

qsub generic.pbs





