#! /bin/bash

if [ $# -ne 5 ]
then
  echo "./exp.strace.read.RCFile.sh <exp> <row group size> <io buffer size> <read column str> <output dir>"
  echo "<exp>: exp1, exp2, exp3, ..."
  exit
fi

EXP=$1
ROW_GROUP_SIZE=$2
IO_BUFFER_SIZE=$3
READ_COLUMN_STR=$4
OUT_DIR=$5

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release

echo "=================================================================="
echo "Row group size:" $ROW_GROUP_SIZE
echo "I/O buffer size:" $IO_BUFFER_SIZE
echo "Read columns str:" $READ_COLUMN_STR
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su #> /dev/null
iostat -d -t $DEVICE
#strace -F -f -ttt -T 
strace -F -f -ttt -T -o $OUT_DIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.out java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFile -t $TABLE -i $DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su #> /dev/null
iostat -d -t $DEVICE
