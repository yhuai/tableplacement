#! /bin/bash

if [ $# -ne 1 ]
then
  echo "./exp.write.sh <exp>"
  echo "<exp>: exp1, exp2, exp3, ..."
  exit
fi

EXP=$1

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release

echo "Task 1: read testing files in the format of RCFile"
for ROW_GROUP_SIZE in $ROW_GROUP_SIZE_LIST
do
    for IO_BUFFER_SIZE in $IO_BUFFER_SIZE_LIST
    do
        for READ_COLUMN_STR in $COLUMNS_STR 
        do
            echo "=================================================================="
            echo "Row group size:" $ROW_GROUP_SIZE
            echo "I/O buffer size:" $IO_BUFFER_SIZE
            echo "Read columns str:" $READ_COLUMN_STR
            echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
            iostat -d -t $DEVICE
            java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFile -t $TABLE -i $DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE
            echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
            iostat -d -t $DEVICE
        done        
    done
done