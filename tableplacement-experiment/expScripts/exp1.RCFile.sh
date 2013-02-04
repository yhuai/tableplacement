#! /bin/bash

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

for COLUMNS_STR in "cfg1:0" "cfg1:0,8" "cfg1:all"
do
  for RG in 1048576 4194304 16777216 67108864 134217728 268435456 536870912
  do
    for BUFFER in 65536 131072 262144 524288
    do
      ./exp.strace.read.RCFile.sh exp1 $RG $BUFFER $COLUMNS_STR ~/Desktop/20120204/exp1/RCFile/ > ~/Desktop/20120204/exp1/RCFile/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.log
    done
  done
done
