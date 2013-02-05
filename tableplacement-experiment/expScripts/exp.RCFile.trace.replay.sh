#! /bin/bash

if [ $# -ne 4 ]
then
  echo "./exp.RCFile.trace.replay.sh <exp> <log output dir> <path to ioscm> <column file group>"
  echo "<exp>: exp1, exp2, exp3, ..."
  echo "You have to checkout IOSCM from https://github.com/s1van/ioscm.git or https://github.com/yhuai/ioscm.git"
  exit
fi

EXP=$1
LOGDIR=$2
IOSCM_PATH=$3
CFG=$4

REPLAYER_PATH=$IOSCM_PATH/tests/

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

cd $REPLAYER_PATH

for READ_COLUMN_STR in "${COLUMNS_STR[@]}"
do
  for ROW_GROUP_SIZE in "${ROW_GROUP_SIZE_LIST[@]}"
  do
    for IO_BUFFER_SIZE in "${IO_BUFFER_SIZE_LIST[@]}"
    do
      STRACE_REPLAY_DIR=$LOGDIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.out.replay/    
      ./TraceReplayer7Batch-default.sh replay 3600 $DIR/$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE/$CFG $STRACE_REPLAY_DIR false 128 1 > $LOGDIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.out.$CFG.replayLog 2>&1
    done
  done
done
