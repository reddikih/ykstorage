#!/bin/sh

JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

CONFIG_PATH=$YKSTORAGE_HOME/$1

if [ ! -e ${CONFIG_PATH} -o ! -f ${CONFIG_PATH} ];then
    echo ${CONFIG_PATH} "is not exist or not a file!"
    exit 1
fi

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

CLASSPATH=${YKSTORAGE_HOME}/bin
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/slf4j-api-1.7.2.jar
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/logback-classic-1.0.9.jar
CLASSPATH=$CLASSPATH$SEPARATOR${YKSTORAGE_HOME}/lib/logback-core-1.0.9.jar

java -cp $CLASSPATH jp.ac.titech.cs.de.ykstorage.service.StorageService $CONFIG_PATH