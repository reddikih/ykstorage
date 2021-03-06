#!/bin/sh

# YKSTORAGE_HOME=
IP_ADDR=
PORT=

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

if [ -f ${YKSTORAGE_HOME}/bin/logback.xml ];then
    mv ${YKSTORAGE_HOME}/bin/logback.xml ${YKSTORAGE_HOME}/bin/_logback.xml
fi

java -cp ${YKSTORAGE_HOME}/bin:${YKSTORAGE_HOME}/lib/junit-4.10.jar:${YKSTORAGE_HOME}/lib/logback-classic-1.0.9.jar:${YKSTORAGE_HOME}/lib/logback-core-1.0.9.jar:${YKSTORAGE_HOME}/lib/slf4j-api-1.7.2.jar org.junit.runner.JUnitCore test.jp.ac.titech.cs.de.ykstorage.frontend.FrontEndTest
