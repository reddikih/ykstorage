#!/bin/sh

DATA_DIR=.
DEV_PREFIX=/dev/sd
USER=`whoami`

if [ -z $USER ]; then
    USER=ecoim
fi

echo "Data directory:" $DATA_DIR
echo "user:" $USER



###
# target device letters:
#
# b c d e f g h i j k l m n o p q r s t u v w x y z aa ab ac ad ae af ag
#
TARGETS=`echo b c d e f g h i j k l m n o p q r s t u v w x y z aa ab ac ad ae af ag`

for i in `echo $TARGETS | cut -d" " -f 1-`
do
    if [ -e ${DEV_PREFIX}${i}1 ]; then
        # echo ${DEV_PREFIX}${i}1
        sudo umount ${DEV_PREFIX}${i}1
    else
        echo ${DEV_PREFIX}${i}1 "is not exist"
    fi
done

for i in `echo $TARGETS | cut -d" " -f 1-`
do
    if [ -e ${DEV_PREFIX}${i}1 ];then
        # echo ${DEV_PREFIX}${i}1 "will be mounted."
        sudo mount -t ext3 ${DEV_PREFIX}${i}1 user/dir/data/sd$i
        sudo chown -R $USER:$USER /user/dir/data/sd$i
    fi
done
