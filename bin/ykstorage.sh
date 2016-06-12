#!/bin/sh

# load include file
if [ "x$YKSTORAGE_INCLUDE" = "x" ]; then
    include=$(dirname $0)/ykstorage.in.sh
    if [ -r "$include" ]; then
	. "$include"
    fi
elif [ -r "$YKSTORAGE_INCLUDE" ]; then
    . "$YKSTORAGE_INCLUDE"
fi


# Use JAVA_HOME if set, otherwise look for java in PATH
# This code copied from cassandra-env.sh.
if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ]; then
    echo "Unable to find java executable. Check JAVA_HOME and PATH environment variables." > /dev/stderr
    exit 1
fi

# check environmental config file PATH
if [ "x$CONF_PATH" != "x" ]; then
    YKSTORAGE_CONF="$CONF_PATH"
fi
if [ -z "$YKSTORAGE_CONF" ]; then
    echo "You must set the YKSTORAGE_CONF vars" >&2
    exit 1
fi

if [ -f "$YKSTORAGE_CONF/ykstorage.properties" ]; then
    . "$YKSTORAGE_CONF/$CONF_NAME"
else
    echo "There is no $CONF_NAME file in $YKSTORAGE_CONF dir."
    exit 1
fi

classname="jp.ac.titech.cs.de.ykstorage.service.StorageService"

# TODO to be service
launch_service() {
    local init="$1"
    local foregroud="$2"
    local class="$3"
    local conf="$4"

    if [ "x$foreground" != "x" ]; then
	exec "$JAVA" $JVM_OPTS -cp "$CLASSPATH" "$class" "$conf"
    else
	exec "$JAVA" $JVM_OPTS -cp "$CLASSPATH" "$class" "$conf" <&- &
    fi

    return $?
}

while true; do
    case "$1" in
	-h)
	    echo "Usage: $0 [--init] conffile"
	    exit 0
	    ;;
	-f)
	    foreground="yes"
	    shift
	    ;;
	--init)
	    initial="yes"
	    shift
	    ;;
	*)
	    conf="$1"
	    if [ -z "$conf" ]; then
		echo "Error parsing arguments!" >&2
		exit 1
	    fi
	    break
	    ;;
    esac
done

# Start up the service
launch_service "$init" "$foreground" "$classname" "$conf"
