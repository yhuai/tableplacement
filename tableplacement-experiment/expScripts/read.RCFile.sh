#! /bin/bash

if [ $# -ne 4 ]
then
  echo "./read.RCFile.sh <input dir> <device> <read column string> <SerDe>"
  echo "read column string format: groupName1:col1,col2\|groupName2:col3\|..."
  echo "in read column string, col must be represented by an integer id. You can also use 'all' to read all columns"
  echo "SerDe: use B for binary SerDe and use T for text SerDe"
  exit
fi

OUT_DIR=$1
DEVICE=$2
READ_COLUMN_STR=$3
SERDE=$4

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
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFile -t ../tableProperties/$TABLE -i $OUT_DIR/ -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su
iostat -d -t $DEVICE
