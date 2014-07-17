#!/bin/sh

DATA_DIR=.
DEV_PREFIX=/dev/sd
USER=`id -un`
GROUP=`id -gn`

if [ -z $USER ]; then
    USER=ecoim
fi

if [ -z $GROUP ]; then
    GROUP=ecoim
fi

echo "Data directory:" $DATA_DIR
echo "User:" $USER "Group:" $GROUP

sudo ls

###
# target device letters:
#
# b c d e f g h i j k l m n o p q r s t u v w x y z aa ab ac ad ae af ag
#
TARGETS=`echo b c d e f g h i j k l m n o p q r s t u v w x y z aa ab ac ad ae af ag`

for i in `echo $TARGETS | cut -d" " -f 1-`
do
    if [ -e ${DEV_PREFIX}${i}1 ];then
	echo "unmount:" ${DEV_PREFIX}${i}1
	sudo umount ${DEV_PREFIX}${i}1 &
    else
	echo ${DEV_PREFIX}${i}1 "is not exist"
    fi
done

wait

for i in `echo $TARGETS | cut -d" " -f 1-`
do
    if [ -e ${DEV_PREFIX}${i}1 ];then

	if [ ! -d ${DATA_DIR}/data/sd$i ]; then
	    echo "mkdir:" ${DATA_DIR}/data/sd$i
	    mkdir -p ${DATA_DIR}/data/sd$i 
	fi

	echo "mount:" ${DEV_PREFIX}${i}1 ${DATA_DIR}/data/sd$i
	sudo mount -t ext3 ${DEV_PREFIX}${i}1 ${DATA_DIR}/data/sd$i

	echo "chown to:" $USER:$GROUP
	sudo chown -R $USER:$GROUP ${DATA_DIR}/data/sd$i
    fi
    
done

echo "================================"
echo "df -h"

df -h
