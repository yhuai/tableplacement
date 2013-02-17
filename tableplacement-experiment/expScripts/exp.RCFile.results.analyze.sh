#! /bin/bash

if [ $# -ne 4 ]
then
  echo "./exp.RCFile.trace.replay.sh <exp> <log output dir> <column file group> <output file>"
  echo "<exp>: exp1, exp2, exp3, ..."
  exit
fi

EXP=$1
LOGDIR=$2
CFG=$3
RESULT_FILE=$4

EXP_COMMON_CONF_PATH="./expConf/common.conf"
echo "Loading parameters from $EXP_COMMON_CONF_PATH"
source $EXP_COMMON_CONF_PATH

EXP_CONF_PATH="./expConf/$EXP.conf"
echo "Loading parameters from $EXP_CONF_PATH"
source $EXP_CONF_PATH

rm $RESULT_FILE

echo "readColumnStr;rowGroupSizeInBytes;IOBufferSizeInBytes;acutalSizeOfDataReadInKB;expectedSizeOfDataReadInMiB;benchElapsedTimeInMS;benchThroughputinMiBPerSecond;replayElapsedTimeInMS;replayTimeOnSystemCallsInMS;replaySizeOfDataReadOnSystemCallsInMiB" >> $RESULT_FILE
for READ_COLUMN_STR in "${COLUMNS_STR[@]}"
do
  for ROW_GROUP_SIZE in "${ROW_GROUP_SIZE_LIST[@]}"
  do
    for IO_BUFFER_SIZE in "${IO_BUFFER_SIZE_LIST[@]}"
    do
      LOG_FILE=$LOGDIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.log
      REPLAY_LOG_FILE=$LOGDIR/strace.$RCFILE_PREFIX.$FILE_PREFIX.c$ROW_COUNT.rg$ROW_GROUP_SIZE.io$IO_BUFFER_SIZE.$READ_COLUMN_STR.out.$CFG.replayLog
      python ../expResultsAnalyzer/expResultsAnalyzer.py $LOG_FILE "$READ_COLUMN_STR;$ROW_GROUP_SIZE;$IO_BUFFER_SIZE" >> $RESULT_FILE
    done
  done
done
