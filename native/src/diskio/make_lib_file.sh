#!/bin/sh
# CAUTION:
# You need to change current directory to ${YKSTORAGE_HOME}/native/src/diskio before you run this script.
# This limitation will fixed in the future.

CC_FILE_PREFIXIES="normaldatadiskio maiddatadiskio raposdadatadiskio cachediskio"

#CC_FILE=$CC_FILE_PREFIX.cc
#OBJ_FILE=$CC_FILE_PREFIX.o
#LIB_FILE=lib$CC_FILE_PREFIX.so

HOSTNAME=`hostname`

if [ $HOSTNAME = "ecoim03" ]; then
    JDK_HOME=/usr/lib/jvm/jdk1.7.0_51
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
for cc_file in `echo $CC_FILE_PREFIXIES | cut -d" " -f 1-`
do
    if [ -e ${cc_file}.cc ]; then
        echo $CC -fPIC -D_GNU_SOURCE $INCLUDE -c ${cc_file}.cc
        $CC -fPIC -D_GNU_SOURCE $INCLUDE -c ${cc_file}.cc
     fi
done

# make shared library
for cc_file in `echo $CC_FILE_PREFIXIES | cut -d" " -f 1-`
do
    if [ -e ${cc_file}.o ]; then
        echo $CC -shared -o lib${cc_file}.so ${cc_file}.o
        $CC -shared -o lib${cc_file}.so ${cc_file}.o
    fi
done

#if [ -f $LIB_FILE ]; then
#    echo cp ./$LIB_FILE ../../../lib
#    cp ./$LIB_FILE ../../../lib
#fi

echo cp ./*.so ../../../lib
cp ./*.so ../../../lib

rm *.so
rm *.o

echo "done successfully."