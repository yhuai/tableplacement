#! /bin/bash

if [ $# -ne 5 ]
then
  echo "./read.RCFile.sh <output dir> <device> <read column string> <SerDe>"
  echo "read column string format: all or col1,col2,col3,..."
  echo "SerDe: use B for binary SerDe and use T for text SerDe"
  exit
fi

OUT_DIR=$1
DEVICE=$2
READ_COLUMN_STR=$3
SERDE=$4
ROW_GROUP_SIZE=$5

TABLE="RCFile.LazyBinaryColumnarSerDe.properties"
FILE_PREFIX="binary"
RCFILE_PREFIX="rcfile"
TREVNI_PREFIX="trevni"

if [ $SERDE == "T" ]
then
  TABLE="RCFile.ColumnarSerDe.properties"
  FILE_PREFIX="text"
fi

ROW_COUNT=3000000
IO_BUFFER_SIZE=65536
echo "Use table property file $TABLE"
echo "Write tmp files to $OUT_DIR in Device $DEVICE ..."

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null

echo "RCFile Read|Binary|Column $READ_COLUMN_STR|IOBuffer $IO_BUFFER_SIZE"
iostat -d -t $DEVICE
#strace -F -f -ttt -T 
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/$TABLE -i $OUT_DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su
iostat -d -t $DEVICE
