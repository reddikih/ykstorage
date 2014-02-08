#!/bin/sh

IP_ADDR=
PORT=

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

if [ -f ${YKSTORAGE_HOME}/bin/logback.xml ];then
    mv ${YKSTORAGE_HOME}/bin/logback.xml ${YKSTORAGE_HOME}/bin/_logback.xml
fi

java -cp ${YKSTORAGE_HOME}/bin:${YKSTORAGE_HOME}/lib/junit4.10.jar:${YKSTORAGE_HOME}/lib/logback-classic-1.0.9.jar:${YKSTORAGE_HOME}/lib/logback-core-1.0.9.jar:${YKSTORAGE_HOME}/lib/slf4j-api-1.7.2.jar jp.ac.titech.cs.de.ykstorage.frontend.FrontEnd