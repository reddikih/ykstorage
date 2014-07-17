#!/bin/sh

############
#
# Usage:
#     ./experiment_201404.sh storage_type worklaod [initial_data]
#
# storage_type - normal, maid or raposda
# workload     - workload file name. do not include the directory paths
# initial_data - workload file of inital data (write only requests). this argument is optional
#

TIME=`date +"%m%d%H%M%S"`

if [ -z "$YKSTORAGE_HOME" ]; then
    YKSTORAGE_HOME=.
fi

${YKSTORAGE_HOME}/tool/fsmount.sh

STORAGE_TYPE=

CLIENT_CONFIG=${YKSTORAGE_HOME}/tool/experiment_201404/config/client.properties

if [ -z "$1" ]; then
    echo "you should specify the storage type (norma, maid or raposda)"
    exit 1
fi
echo "Storage type: " $STORAGE_TYPE

if [ -z "$2" ]; then
    echo "you should specify the workload file name"
    exit 1
fi
WORKLOAD=${YKSTORAGE_HOME}/workload/$2
echo "Workload file: " $WORKLOAD

if [ -z "$3" ]; then
    INITIAL_DATA=${YKSTORAGE_HOME}/workload/workload.initial.100K
else
    INITIAL_DATA=${YKSTORAGE_HOME}/workload/$3
fi
echo "Initial data: " $INITAIL_DATA

if [ $1 = "normal" ]; then
    STORAGE_TYPE=Normal
elif [ $1 = "maid" ]; then
    STORAGE_TYPE=MAID
elif [ $1 = "raposda" ]; then
    STORAGE_TYPE=RAPoSDA
fi

if [ ! -d ${YKSTORAGE_HOME}/log ]; then
    mkdir ${YKSTORAGE_HOME}/log
    echo "Created log directory."
fi

### executes the storage system ###

# start server for load the initial data
SERVER_CONFIG=${YKSTORAGE_HOME}/tool/experiment_201404/config/initial${STORAGE_TYPE}.properties
${YKSTORAGE_HOME}/tool/start_ykstorage.sh $SERVER_CONFIG > /dev/null 2>&1 &

sleep 2s

# execute client for load initial data by sigle thread mode.
${YKSTORAGE_HOME}/tool/singleclient.sh $CLIENT_CONFIG $INITIAL_DATA

# remove cached data from cache disks
rm -rf ${YKSTORAGE_HOME}/data/sdb ${YKSTORAGE_HOME}/data/sdc

# start server for experiment
SERVER_CONFIG=${YKSTORAGE_HOME}/tool/experiment_201404/config/${STORAGE_TYPE}.properties
${YKSTORAGE_HOME}/tool/start_ykstorage.sh $SERVER_CONFIG > \
    ${YKSTORAGE_HOME}/log/${STORAGE_TYPE}_server_${TIME}.log &

sleep 2s

# execute client requests based on specified workload file
${YKSTORAGE_HOME}/tool/multiclient.sh $CLIENT_CONFIG $WORKLOAD > \
    ${YKSTORAGE_HOME}/log/${STORAGE_TYPE}_client_${TIME}.log
