#!/usr/bin/python
## trace should be collected through strace -F -f -ttt -T -o <File>
import sys
import os

if (len(sys.argv) != 4):
    print "usage: python expResultsAnalyzer.py <log file> <replay file> <prefix>"
    sys.exit(1);
    
logFile = sys.argv[1];
replayFile = sys.argv[2];
prefix = sys.argv[3];

ifn = open(logFile, 'r');
lines = ifn.readlines();

## Calculate the size of data actually read from the device
startSizeInKB = int(lines[24].strip().split()[4]);
endSizeInKB = int(lines[-2].strip().split()[4]);
actualDataSizeInKB = endSizeInKB - startSizeInKB;
# get measures from log file
expectedDataSizeInMiB = float(lines[-24].strip().split()[5]);
elapsedTimeInMS = float(lines[-23].strip().split()[3]);
throughput = float(lines[-15].strip().split()[2]); # MiB/s

ifn.close();


ifn = open(replayFile, 'r');
lines = ifn.readlines();
startTS = [];
endTS = [];
totalTimeOnSystemCallInMS = 0;
totalDataSizeReadFromSystemCallInMiB = 0;
for line in lines[9:-1]:
    terms = line.strip().split();
    startTS.append(long(terms[9]));
    endTS.append(long(terms[10]));
    totalTimeOnSystemCallInMS += long(terms[11]);
    totalDataSizeReadFromSystemCallInMiB += long(terms[13]);
totalTimeOnSystemCallInMS = float(totalTimeOnSystemCallInMS) / 1000000
totalDataSizeReadFromSystemCallInMiB = float(totalDataSizeReadFromSystemCallInMiB) / 1024/1024
replayElapsedTimeInMS = float(max(endTS) - min(startTS))/1000000;
ifn.close();

sys.stdout.write(prefix + \
                 "," + str(actualDataSizeInKB) + \
                 "," + str(expectedDataSizeInMiB) + \
                 "," + str(elapsedTimeInMS) + \
                 "," + str(throughput) + \
                 "," + str(replayElapsedTimeInMS) + \
                 "," + str(totalTimeOnSystemCallInMS) + \
                 "," + str(totalDataSizeReadFromSystemCallInMiB) + '\n');