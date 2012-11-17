#! /bin/bash

if [ $# -ne 4 ]
then
  echo "./write.RCFile.sh <output dir> <device> <read column string> <SerDe>"
  echo "read column string format: all or col1,col2,col3,..."
  echo "SerDe: use B for binary SerDe and use T for text SerDe"
  exit
fi

OUT_DIR=$1
DEVICE=$2
SERDE=$3
ROW_GROUP_SIZE=$4

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

echo "RCFile Write|Binary|RC $ROW_COUNT|RG $ROW_GROUP_SIZE"
iostat -d -t $DEVICE
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/$TABLE -o $OUT_DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -c $ROW_COUNT -p hive.io.rcfile.record.buffer.size $ROW_GROUP_SIZE
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su
iostat -d -t $DEVICE
