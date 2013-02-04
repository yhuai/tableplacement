#! /bin/bash

if [ $# -ne 2 ]
then
  echo "./exp.write.sh <exp> <log output dir>"
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
  for ROW_GROUP_SIZE in "${ROW_GROUP_SIZE_LIST[@]}"
  do
    for IO_BUFFER_SIZE in "${IO_BUFFER_SIZE_LIST[@]}"
    do
      ./exp.strace.read.RCFile.sh exp1 $ROW_GROUP_SIZE $IO_BUFFER_SIZE $READ_COLUMN_STR $LOGDIR > $LOGDIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.log 2>&1
    done
  done
done
