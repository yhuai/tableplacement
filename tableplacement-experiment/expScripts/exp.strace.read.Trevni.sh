#! /bin/bash

if [ $# -ne 5 ]
then
  echo "./exp.strace.read.Trevni.sh <exp> <io buffer size> <read column str> <access pattern> <strace output dir>"
  echo "<exp>: exp1, exp2, exp3, ..."
  echo "<access pattern>: r is row-oriented access; c is column-oriented access"
  exit
fi

EXP=$1
IO_BUFFER_SIZE=$2
READ_COLUMN_STR=$3
ACCESS=$4
OUT_DIR=$5

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

# override TREVNI_TEST_CLASS based on input parameter
TREVNI_TEST_CLASS="ReadTrevniColumnOriented"
if [ $ACCESS == "r" ]
then  	  	
  TREVNI_TEST_CLASS="ReadTrevniRowOriented"
fi

echo "Printing system infomation ..."
uname -a
cat /etc/lsb-release

echo "=================================================================="
echo "I/O buffer size:" $IO_BUFFER_SIZE
echo "Read columns str:" $READ_COLUMN_STR
echo "Trevni test class:" $TREVNI_TEST_CLASS
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su #> /dev/null
iostat -d -t -p
strace -F -f -ttt -T -o $OUT_DIR/strace.$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT.ap$ACCESS.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.out java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar $TREVNI_TEST_CLASS -t $TABLE -i $DIR/$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT -p read.column.string $READ_COLUMN_STR -p io.file.buffer.size $IO_BUFFER_SIZE $OTHER_PROPERTIES
echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su #> /dev/null
iostat -d -t -p
