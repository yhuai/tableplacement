#! /bin/bash

if [ $# -ne 2 ]
then
  echo "./exp1.read.sh <inputput dir> <device>"
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

ROW_GROUP_SIZE=1048576
IO_BUFFER_SIZE=524288
READ_COLUMN_STR='cfg1:3'
echo "=================================================================="
echo "Row group size:" $ROW_GROUP_SIZE
echo "I/O buffer size:" $IO_BUFFER_SIZE
echo "Read columns str:" $READ_COLUMN_STR
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t $DEVICE
strace -F -f -ttt -T java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFile -t $TABLE -i $OUT_DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t $DEVICE
