#! /bin/bash

if [ $# -ne 2 ]
then
  echo "./exp1.write.sh <output dir> <device>"
  exit
fi

OUT_DIR=$1
DEVICE=$2

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release


ROW_COUNT=100000000     
IO_BUFFER_SIZE=524288  # 512KiB buffer size
FILE_PREFIX="binary"
RCFILE_PREFIX="rcfile"
TREVNI_PREFIX="trevni"

TABLE="../tableProperties/LazyBinaryColumnarSerDe/t1-singleFile-noColumnGroup.properties"
echo "Task 1: write testing files in the format of RCFile"
for ROW_GROUP_SIZE in 1048576 4194304 16777216 67108864 268435456 1073741824
do
    echo "=================================================================="
    echo "Row group size:" $ROW_GROUP_SIZE
    echo "I/O buffer size:" $IO_BUFFER_SIZE    
    echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
    iostat -d -t $DEVICE
    java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFile -t $TABLE -o $OUT_DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -c $ROW_COUNT -p hive.io.rcfile.record.buffer.size $ROW_GROUP_SIZE
    echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
    iostat -d -t $DEVICE
done

echo "=================================================================="
echo "Task 2: write testing files in the format of Trevni"
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t $DEVICE
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteTrevni -t $TABLE -o $OUT_DIR/$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT -c $ROW_COUNT
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t $DEVICE
