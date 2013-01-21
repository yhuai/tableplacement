#! /bin/bash

if [ $# -ne 3 ]
then
  echo "./write.Trevni.sh <output dir> <device> <SerDe>"
  echo "SerDe: use B for binary SerDe and use T for text SerDe"
  exit
fi

OUT_DIR=$1
DEVICE=$2
SERDE=$3

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
IO_BUFFER_SIZE=524288
echo "Use table property file $TABLE"
echo "Write tmp files to $OUT_DIR in Device $DEVICE ..."

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null

echo "Trevni Read|Binary|Column $READ_COLUMN_STR|IOBuffer $IO_BUFFER_SIZE"
iostat -d -t $DEVICE
#strace -F -f -ttt -T 
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteTrevni -t ../tableProperties/$TABLE -o $OUT_DIR/$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT -c $ROW_COUNT
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su
iostat -d -t $DEVICE
