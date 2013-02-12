#!/usr/bin/python
## trace should be collected through strace -F -f -ttt -T -o <File>
import sys
import os

if (len(sys.argv) != 3):
    print "usage: python expResultsAnalyzer.py <replay file> <prefix>"
    sys.exit(1);

replayFile = sys.argv[1];
prefix = sys.argv[2];

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

sys.stdout.write(prefix + 
                 "|" + str(replayElapsedTimeInMS) + \
                 "|" + str(totalTimeOnSystemCallInMS) + \
                 "|" + str(totalDataSizeReadFromSystemCallInMiB) + '\n');