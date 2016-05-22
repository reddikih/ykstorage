#!/bin/sh

# TODO move to env file
MOUNT_POINT="/mnt/ykstorage"
DATA_DIR_PREFIX=$MOUNT_POINT/data
SYS_DISK="a"

active_disks=$(ls /dev | awk '/sd[a-z]+[0-9]/' | awk "! /sd[$SYS_DISK]/{print}" | awk 'gsub(/1/,"")')

for disk in $active_disks
do
    # unmount
    printf "unmont %s, " /dev/$disk
    sudo umount /dev/${disk}1 2>/dev/null
    if [ $? -ne 0 ];then printf "/dev/$disk is not mounted";fi
    echo ""
done
