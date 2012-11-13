#! /bin/bash

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -o ~/rcfile/lazy.c3000000.rg1048576 -c 3000000 -p hive.io.rcfile.record.buffer.size 1048576

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -o ~/rcfile/lazy.c3000000.rg4194304 -c 3000000 -p hive.io.rcfile.record.buffer.size 4194304

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -o ~/rcfile/lazy.c3000000.rg67108864 -c 3000000 -p hive.io.rcfile.record.buffer.size 67108864


java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -o ~/rcfile/text.c3000000.rg1048576 -c 3000000 -p hive.io.rcfile.record.buffer.size 1048576

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -o ~/rcfile/text.c3000000.rg4194304 -c 3000000 -p hive.io.rcfile.record.buffer.size 4194304

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar WriteRCFileToLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -o ~/rcfile/text.c3000000.rg67108864 -c 3000000 -p hive.io.rcfile.record.buffer.size 67108864



java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -i ~/rcfile/lazy.c3000000.rg1048576 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -i ~/rcfile/lazy.c3000000.rg4194304 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.LazyBinaryColumnarSerDe.properties -i ~/rcfile/lazy.c3000000.rg67108864 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -i ~/rcfile/text.c3000000.rg1048576 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -i ~/rcfile/text.c3000000.rg4194304 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288

java -jar ../target/tableplacement-experiment-0.0.1-SNAPSHOT.jar ReadRCFileFromLocal -t ../tableProperties/RCFile.ColumnarSerDe.properties -i ~/rcfile/text.c3000000.rg67108864 -p rcfile.read.column.string 1 -p io.file.buffer.size 524288
