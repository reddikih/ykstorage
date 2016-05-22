#!/bin/sh
# to check mount status
# cat /etc/mtab

# TODO move to env file
MOUNT_POINT="/mnt/ykstorage"
DATA_DIR_PREFIX=$MOUNT_POINT/data
SYS_DISK="a"

USER=`id -un`
GROUP=`id -gn`

active_disks=$(ls /dev | awk '/sd[a-z]+[0-9]/' | awk "! /sd[$SYS_DISK]/{print}" | awk 'gsub(/1/,"")')

if [ ! -d $MOUNT_POINT ];then
    sudo mkdir -p $MOUNT_POINT
fi

for disk in $active_disks
do
    # echo $disk
    data_dir=$DATA_DIR_PREFIX/$disk

    # # unmount
    # printf "unmont %s, " /dev/$disk
    # sudo umount /dev/${disk}1 2>/dev/null
    
    # create data dir
    if [ ! -d $data_dir ];then
	printf "make data dir %s, " $data_dir
	sudo mkdir -p $data_dir
    fi

    # mount
    printf "mount(ext4) %s to %s, " /dev/$disk $data_dir
    sudo mount -t ext4 /dev/${disk}1 $data_dir

    # change owner
    printf "chown to %s:%s\n" $USER $GROUP
    sudo chown -R $USER:$GROUP $data_dir
done