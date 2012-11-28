# README
This project aims to evaluate different table placement formats in the ecosystem of [Apache Hadoop](http://hadoop.apache.org/). This work is still in development and is in a very primitive stage. Also, currently, this work is not well documented.

# Covered placement formats
RCFile in [Apache Hive](http://hive.apache.org/)

Trevni in [Apache Avro](http://avro.apache.org/)

I will add new formats in future...

# Build
1. Install [Apache Maven](http://maven.apache.org/) (Version 3.0.4).
2. If you want to use Avro 1.7.3-SNAPSHOT, please check it out from git (`git clone https://github.com/apache/avro.git`) and intall it into your local repository (`mvn clean install`). 
3. In the root of the SideWalk directory, if you want to use Avro 1.7.3-SNAPSHOT, then execute `mvn clean package -P avro-1.7.3 -DskipTests`. Otherwise, `mvn clean package -DskipTests`

### Notes
* During my development, Oracle Java (Version 1.6.0_26) is used.

# Test
Test all cases: `mvn test`

Test a single case: `mvn -Dtest=<class_name> test`

# Execute experiments
In `tableplacement-experiment/expScripts`, there are four scripts which are used to execute experiments of read/write operations with RCFile and Trevni.
Please execute those scripts in the directory of `tableplacement-experiment/expScripts`.
Every script has 5 steps:

1. Sync data to disk and clear OS buffer cache
2. Call `iostat` to print statistics of the device you will perform the experiment on. (Please install iostat first.)
3. Invoke java programs to perform the experiment.
4. Sync data to disk and clear OS buffer cache again.
5. Call `iostat` again.

Here are short descriptions of scripts in `tableplacement-experiment/expScripts`.

* `write.Trevni.sh` generate a file with the format of Trevni.
   Usage: `sudo ./write.Trevni.sh <output dir> <device> <SerDe>`
* `read.Trevni.sh` read a file from a file with the format of Trevni.
   Usage: `sudo ./read.Trevni.sh <input dir> <device> <read column string> <SerDe>`
* `write.RCFile.sh` generate a file with the format of RCFile.
   Usage: `sudo ./write.RCFile.sh <output dir> <device> <SerDe> <row group size>`
* `read.RCFile.sh` read a file from a file with the format of Trevni.
   Usage: `sudo ./read.RCFile.sh <input dir> <device> <read column string> <SerDe> <row group size>`

### Notes
* `<output dir>` and `<input dir>` are dirs used to store files. Filenames will be automatically generated.
* `<device>` is the storage device which experiments will work on. It should be something like `/dev/sda`
* In current implementation, I used ColumnarSerDe in Hive for serilization and deserilization. `<SerDe>` represents which type of ColumnarSerDe will be used. `B` means `LazyBinaryColumnarSerDe`, which is binary-based, and `T` means `ColumnarSerDe`, which is text-based.
* In experiments for read operations, you can specify which column(s) you want to read through `<read column string>`. For example, `0,1,2,5` means to read first three columns and sixth column. To read all columns, just use string `all`.
* The table which will be generated is described in `tableplacement-experiment/tableProperties/RCFile.LazyBinaryColumnarSerDe.properties`. The table has 6 int columns, the value of which is randomly picked from 0 to 9999, 6 string columns, the length of each string is 30, and 1 map column, the size of which is 10. The type of keys in the map column is string and the length of each key string is 4. The value type of values in the map column is int and every int value is randomly picked from 0 to 2147483646.
* In current scripts, generated files will have 3000000 rows.
* `io.file.buffer.size` in every experiment has been set to 65536 bytes (64KiB).
* `<row group size>` is the row group size used for RCFile.


#Developers
Yin Huai  [http://www.cse.ohio-state.edu/~huai/](http://www.cse.ohio-state.edu/~huai/)
