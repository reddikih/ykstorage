#!/bin/sh

JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8
JVM_OPTION="-Xms1000m -Xmx1500m"

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

if [ -f ${YKSTORAGE_HOME}/bin/logback.xml ];then
    mv ${YKSTORAGE_HOME}/bin/logback.xml ${YKSTORAGE_HOME}/bin/_logback.xml
fi

CYGWIN=false
case "$(uname)" in
CYGWIN*) CYGWIN=true;;
esac

if $CYGWIN; then
    SEPARATOR=";"
else
    SEPARATOR=":"
fi

#CLASSPATH=${YKSTORAGE_HOME}/bin
CLASSPATH=${YKSTORAGE_HOME}/build/libs/*
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/slf4j-api-1.7.2.jar
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/logback-classic-1.0.9.jar
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/logback-core-1.0.9.jar

CONFIG_FILE=${YKSTORAGE_HOME}/config/client_test/client.properties
WORKLOAD_FILE=${YKSTORAGE_HOME}/workload/client_test/workload.initial1000
java -cp $CLASSPATH $JVM_OPTION jp.ac.titech.cs.de.ykstorage.cli.SingleClient ${CONFIG_FILE} ${WORKLOAD_FILE}
