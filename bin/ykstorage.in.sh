# including file

if [ "x$YKSTORAGE_HOME" = "x" ]; then
    YKSTORAGE_HOME="$(dirname $0)/.."
fi

if [ "x$YKSTORAGE_CONF" = "x" ]; then
    YKSTORAGE_CONF="$YKSTORAGE_HOME/conf"
fi

ykstorage_bin="$YKSTORAGE_HOME/build/classes/main"

ykstorage_datadir="$YKSTORAGE_HOME/data"

# The java classpath (required)
CLASSPATH="$YKSTORAGE_CONF:$ykstorage_bin"
CLASSPATH="$CLASSPATH:$YKSTORAGE_HOME/build/libs/*"

# Added jni libraries
JAVA_OPTS="$JAVA_OPTS:-Djava.library.path=$YKSTORAGE_HOME/lib"


