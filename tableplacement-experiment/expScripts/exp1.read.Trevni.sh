#! /bin/bash

if [ $# -ne 2 ]
then
  echo "./exp1.read.Trevni.sh <inputput dir> <device>"
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
echo "Task 1: read testing files in the format of Trevni"
for IO_BUFFER_SIZE in 65536 131072 262144 524288 1048576
do
    for READ_COLUMN_STR in "cfg1:3" "cfg1:1,3,5,7" "cfg1:all" 
    do
        echo "=================================================================="
        echo "Row group size:" $ROW_GROUP_SIZE
        echo "I/O buffer size:" $IO_BUFFER_SIZE
        echo "Read columns str:" $READ_COLUMN_STR
        echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
        iostat -d -t $DEVICE
        java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadTrevni -t $TABLE -i $OUT_DIR/$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
        echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
        iostat -d -t $DEVICE
    done        
done
