#!/bin/sh

JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8
JVM_OPTION="-Xms12000m -Xmx12000m"

echo "YKSTORAGE_HOME = " $YKSTORAGE_HOME

CONFIG_PATH=$YKSTORAGE_HOME/config/client_test/RAPoSDA.properties

if [ ! -e ${CONFIG_PATH} -o ! -f ${CONFIG_PATH} ];then
    echo ${CONFIG_PATH} "is not exist or not a file!"
    exit 1
fi

. $YKSTORAGE_HOME/tool/fsmount.sh

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

echo "start StorageService." `date` 1>&2

# java -cp $CLASSPATH jp.ac.titech.cs.de.ykstorage.service.StorageService $CONFIG_PATH
# run with JNI I/O
java -Djava.library.path=$YKSTORAGE_HOME/lib -cp $CLASSPATH $JVM_OPTION jp.ac.titech.cs.de.ykstorage.service.StorageService $CONFIG_PATH

echo "end StorageService.." `date` 1>&2

tree data > /dev/null 2>&1

