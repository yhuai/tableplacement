#!/usr/bin/python
## trace should be collected through strace -F -f -ttt -T -o <File>
import sys
import os

if (len(sys.argv) != 3):
    print "usage: python expResultsAnalyzer.py <log file> <prefix>"
    sys.exit(1);
    
logFile = sys.argv[1];
prefix = sys.argv[2];

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

sys.stdout.write(prefix + \
                 "|" + str(actualDataSizeInKB) + \
                 "|" + str(expectedDataSizeInMiB) + \
                 "|" + str(elapsedTimeInMS) + \
                 "|" + str(throughput) + '\n');