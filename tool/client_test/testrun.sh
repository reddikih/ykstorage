#!/bin/sh

TIME=`date +"%m%d%H%M%S"`

${YKSTORAGE_HOME}/tool/fsmount.sh

if test $1 = "normal"
then
  ${YKSTORAGE_HOME}/tool/client_test/server/start_initialNormal.sh &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client_initial.sh
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client_initial.sh
  fi
  
  ${YKSTORAGE_HOME}/tool/client_test/server/start_Normal.sh > ${YKSTORAGE_HOME}/log/Normal_server_${TIME}.log &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client.sh > ${YKSTORAGE_HOME}/log/Normal_client_${TIME}.log
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client.sh > ${YKSTORAGE_HOME}/log/Normal_client_${TIME}.log
  fi
fi


if test $1 = "maid"
then
  ${YKSTORAGE_HOME}/tool/client_test/server/start_initialMAID.sh &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client_initial.sh
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client_initial.sh
  fi
  
  rm -rf ${YKSTORAGE_HOME}/data/sdb ${YKSTORAGE_HOME}/data/sdc
  
  ${YKSTORAGE_HOME}/tool/client_test/server/start_MAID.sh > ${YKSTORAGE_HOME}/log/MAID_server_${TIME}.log &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client.sh > ${YKSTORAGE_HOME}/log/MAID_client_${TIME}.log
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client.sh > ${YKSTORAGE_HOME}/log/MAID_client_${TIME}.log
  fi
fi


if test $1 = "raposda"
then
  ${YKSTORAGE_HOME}/tool/client_test/server/start_initialRAPoSDA.sh &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client_initial.sh
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client_initial.sh
  fi
  
  rm -rf ${YKSTORAGE_HOME}/data/sdb ${YKSTORAGE_HOME}/data/sdc
  
  ${YKSTORAGE_HOME}/tool/client_test/server/start_Normal.sh > ${YKSTORAGE_HOME}/log/RAPoSDA_server_${TIME}.log &
  sleep 3s
  
  if test $2 = "5"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/start_client.sh > ${YKSTORAGE_HOME}/log/RAPoSDA_client_${TIME}.log
  elif test $2 = "10"
  then
    ${YKSTORAGE_HOME}/tool/client_test/client/10minutes/start_client.sh > ${YKSTORAGE_HOME}/log/RAPoSDA_client_${TIME}.log
  fi
fi
