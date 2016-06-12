MAX_HEAP_SIZE="12000M"
HEAP_NEWSIZE="3000M"

JVM_OPTS="$JVM_OPTS -ea"

# thread priority setting
# this code from casandra-env.sh
# a lower priority to avoid interfering with client workload
JVM_OPTS="$JVM_OPTS -XX:+UseThreadPriorities"
# allows lowering thread priority without being root.  see
# http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workaround.html
JVM_OPTS="$JVM_OPTS -XX:ThreadPriorityPolicy=42"

JVM_OPTS="$JVM_OPTS -Xms${MAX_HEAP_SIZE}"
JVM_OPTS="$JVM_OPTS -Xmx${MAX_HEAP_SIZE}"
JVM_OPTS="$JVM_OPTS -Xmn${HEAP_NEWSIZE}"
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError"

# set jvm HeapDumpPath with YKSTORAGE_HEAPDUMP_DIR
if [ "x$YKSTORAGE_HEAPDUMP_DIR" != "x" ]; then
    JVM_OPTS="$JVM_OPTS -XX:HeapDumpPath=$YKSTORAGE_HEAPDUMP_DIR/ykstorage-`date +%s`-pid$$.hprof"
fi

# Per-thread stack size.
JVM_OPTS="$JVM_OPTS -Xss256k"

# GC logging options
JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails"
JVM_OPTS="$JVM_OPTS -XX:+PrintGCDateStamps"
JVM_OPTS="$JVM_OPTS -XX:+PrintHeapAtGC"
JVM_OPTS="$JVM_OPTS -XX:+PrintTenuringDistribution"
JVM_OPTS="$JVM_OPTS -XX:+PrintGCApplicationStoppedTime"
JVM_OPTS="$JVM_OPTS -XX:+PrintPromotionFailure"

JVM_OPTS="$JVM_OPTS -Xloggc:${YKSTORAGE_HOME}/logs/gc.log"
JVM_OPTS="$JVM_OPTS -XX:+UseGCLogFileRotation"
JVM_OPTS="$JVM_OPTS -XX:NumberOfGCLogFiles=10"
JVM_OPTS="$JVM_OPTS -XX:GCLogFileSize=20M"
