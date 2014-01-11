#!/bin/sh

CC_FILE_PREFIX=datadiskio
CC_FILE=$CC_FILE_PREFIX.cc
OBJ_FILE=$CC_FILE_PREFIX.o
LIB_FILE=lib$CC_FILE_PREFIX.so

HOSTNAME=`hostname`

if [ $HOSTNAME = "ecoim03" ]; then
    JDK_HOME=/usr/lib/jvm/java-6-sun
    CC=g++ 
    INCLUDE="-I$JDK_HOME/include -I$JDK_HOME/include/linux"
elif [ $HOSTNAME = "camelia" ]; then
    JDK_HOME=/usr/lib/jvm/java-7-openjdk-amd64
    CC=gcc
    INCLUDE="-I$JDK_HOME/include"
else
    echo "please run this shell script on either ecoim03 or camelia."
    exit 1 # anomaly terminate.
fi

# compile cc file
echo $CC -fPIC -D_GNU_SOURCE $INCLUDE -c $CC_FILE
$CC -fPIC -D_GNU_SOURCE $INCLUDE -c $CC_FILE

# make shared library
echo $CC -shared -o $LIB_FILE $OBJ_FILE
$CC -shared -o $LIB_FILE $OBJ_FILE

if [ -f $LIB_FILE ]; then
    echo cp ./$LIB_FILE ../../../lib
    cp ./$LIB_FILE ../../../lib
fi

echo "done successfully."