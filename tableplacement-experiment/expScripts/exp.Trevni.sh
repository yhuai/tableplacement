#! /bin/bash

if [ $# -ne 2 ]
then
  echo "./exp.Trevni.sh <exp> <log output dir>"
  echo "<exp>: exp1, exp2, exp3, ..."
  exit
fi

EXP=$1
LOGDIR=$2

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

for READ_COLUMN_STR in "${COLUMNS_STR[@]}"
do
  for ACCESS_PATTERN in "r" "c"
  do
    for IO_BUFFER_SIZE in "${IO_BUFFER_SIZE_LIST[@]}"
    do
      ./exp.strace.read.Trevni.sh $EXP $IO_BUFFER_SIZE $READ_COLUMN_STR $ACCESS_PATTERN $LOGDIR > $LOGDIR/strace.$TREVNI_PREFIX.$FILE_PREFIX.c$ROW_COUNT.ap$ACCESS_PATTERN.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.log 2>&1
    done
  done
done
