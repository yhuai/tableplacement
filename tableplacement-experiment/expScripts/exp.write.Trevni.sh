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
   
echo "Write I/O buffer size:" $WRITE_IO_BUFFER_SIZE  

echo "write testing files in the format of Trevni"
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t -p
java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteTrevni -t $TABLE -o $DIR/$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT -c $ROW_COUNT -p io.file.buffer.size $WRITE_IO_BUFFER_SIZE $OTHER_PROPERTIES
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su > /dev/null
iostat -d -t -p
