#!/usr/bin/python
## trace should be collected through strace -F -f -ttt -T -o <File>
import sys

if (len(sys.argv) != 3):
    print "usage: python strace_analyzer.py <file> <file descriptor>"
    sys.exit(1);
    
file = sys.argv[1];
fd = sys.argv[2];

print "Analyzing read, lseek, and pread system calls in " + file + \
      " for file descriptpr " + fd;


unfinished = [0, 0, 0]; ## unfinished lseek, read, and pread
readOps = [];

ifn = open(file, 'r');
ts = -1;
hasUnfinishedCall = False;
callNeededToResume = [];
for line in ifn:
    terms = line.strip().split();
    pid = terms[0];
    tsStr = terms[1];
    call = terms[2];
    if (call == "lseek(" + fd + ","):
        if (terms[-2] == "<unfinished"):
            unfinished[0] += 1;
            if (hasUnfinishedCall):
                print "We already have a unfinished call. Fix the program"
                sys.exit(1);
            hasUnfinishedCall = True;
            callNeededToResume = ["lseek", tsStr];
            continue;
        time = terms[-1][1:-1];
        offset = terms[-2];
        readOps.append(["lseek", tsStr, time, offset]);
    elif (call == "read(" + fd + ","):
        if (terms[-2] == "<unfinished"):
            unfinished[1] += 1;
            if (hasUnfinishedCall):
                print "We already have a unfinished call. Fix the program"
                sys.exit(1);
            hasUnfinishedCall = True;
            callNeededToResume = ["read", tsStr];
            continue;
        time = terms[-1][1:-1];
        size = terms[-2];
        readOps.append(["read", tsStr, time, size]);
    elif (call == "pread(" + fd + ","):
        if (terms[-2] == "<unfinished"):
            unfinished[2] += 1;
            if (hasUnfinishedCall):
                print "We already have a unfinished call. Fix the program"
                sys.exit(1);
            hasUnfinishedCall = True;
            callNeededToResume = ["pread", tsStr];
            continue;
        time = terms[-1][1:-1];
        offset = terms[-4][:-1];
        size = terms[-2];
        readOps.append(["pread", tsStr, time, offset, size]);
    elif (hasUnfinishedCall):
        if (terms[3] == callNeededToResume[0] and terms[4] == "resumed>"):
            time = terms[-1][1:-1];
            if (callNeededToResume[0] == "lseek"):
                offset = terms[-2];
                readOps.append([callNeededToResume[0], callNeededToResume[1], time, offset]);
            elif (callNeededToResume[0] == "read"):
                size = terms[-2];
                readOps.append([callNeededToResume[0], callNeededToResume[1], time, size]);
            elif (callNeededToResume[0] == "pread"):
                offset = terms[-4][:-1];
                size = terms[-2];
                readOps.append([callNeededToResume[0], callNeededToResume[1], time, offset, size]);
            hasUnfinishedCall = False;
ifn.close();

if (len(readOps) == 0):
    sys.exit();

startTs = float(readOps[0][1]);
offset = 0;
ofn1 = open(file + ".callTimeSeries", 'w');
ofn2 = open(file + ".read.plot", 'w');
ofn3 = open(file + ".read.R.plot", 'w');
ofn4 = open(file + ".replay", 'w');
print>>ofn3, "startTS", "startPos", "endTS", "endPos";
for entry in readOps:
    call = entry[0];
    ts = float(entry[1]) - startTs;
    time = entry[2];
    if (call == "lseek"):
        offset = long(entry[3]);
        print>>ofn1, str(ts), call, time, offset;
    elif (call == "read"):
        size = entry[3];
        print>>ofn1, str(ts), call, time, size;
        readStartTS = ts;
        readEndTS = readStartTS + float(time);
        readStartPos = offset;
        readEndPos = readStartPos + long(size);
        offset = readEndPos;
        print>>ofn2, str(readStartTS), str(readStartPos);
        print>>ofn2, str(readEndTS), str(readEndPos);
        print>>ofn3, str(readStartTS), str(readStartPos), str(readEndTS), str(readEndPos);
        print>>ofn4, str(readStartPos) + "|" + str(size) + "|r|0";
    elif (call == "pread"):
        thisOffset = long(entry[3]);
        size = entry[4];
        print>>ofn1, str(ts), call, time, thisOffset, size;
        readStartTS = ts;
        readEndTS = readStartTS + float(time);
        readStartPos = thisOffset;
        readEndPos = readStartPos + long(size);
        print>>ofn2, str(readStartTS), str(readStartPos);
        print>>ofn2, str(readEndTS), str(readEndPos);
        print>>ofn3, str(readStartTS), str(readStartPos), str(readEndTS), str(readEndPos);
        print>>ofn4, str(readStartPos) + "|" + str(size) + "|r|0";
ofn1.close();
ofn2.close();
ofn3.close();          
