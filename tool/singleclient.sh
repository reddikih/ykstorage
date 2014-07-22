#!/bin/sh

JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8
JVM_OPTION="-Xms1000m -Xmx1500m"

if [ -z "$YKSTORAGE_HOME" ]; then
    YKSTORAGE_HOME=.
fi

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

CONFIG=$1
WORKLOAD=$2

if [ -z "$CONFIG" ]; then
    echo "you should specify client config file"
    exit 1
fi
if [ -z "$WORKLOAD" ]; then
    echo "you should specify workload file path"
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

java -cp $CLASSPATH $JVM_OPTION jp.ac.titech.cs.de.ykstorage.cli.SingleClient $CONFIG $WORKLOAD
