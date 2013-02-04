# README
This project aims to evaluate different table placement formats in the ecosystem of [Apache Hadoop](http://hadoop.apache.org/). This work is still in development and is in a very primitive stage. Also, currently, this work is not well documented.

# Table placement formats
RCFile in [Apache Hive](http://hive.apache.org/) version 0.9.0 

Trevni in [Apache Avro](http://avro.apache.org/) version 1.7.3 and 1.7.4-SNAPSHOT

I will add new formats in future...

# Build
1. Install [Apache Maven](http://maven.apache.org/) (Version 3.0.4).
2. Install Zebra into your local maven repository. In the root directory of this project, execute `mvn install:install-file -Dfile=./lib/zebra-0.8.0-dev.jar -DgroupId=org.apache.pig -DartifactId=zebra -Dversion=0.8-dev -Dpackaging=jar`.
2. If you want to use Avro 1.7.4-SNAPSHOT, please check it out from git (`git clone https://github.com/apache/avro.git`) and intall it into your local repository (`mvn clean install`). 
3. In the root of the SideWalk directory, if you want to use Avro 1.7.4-SNAPSHOT, then execute `mvn clean package -P avro-1.7.4 -DskipTests`. Otherwise, `mvn clean package -DskipTests`

### Notes
* From 2/4/2013, I have been using OpenJDK (Version 1.7.0_09).
* Commits before 2/4/2013 were developed and tested with Oracle Java (Version 1.6.0_26).

# Test
Test all cases: `mvn test`

Test a single case: `mvn -Dtest=<class_name> test`

#Table Property File
A table is defined by a table property file. There are a few examples located in '/tableplacement-experiment/tableProperties'. Properties can be defined in this file are introduced below.

[TODO: add explainations of a table property file]

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
[TODO: explainations of scripts]


#Developers
Yin Huai  [http://www.cse.ohio-state.edu/~huai/](http://www.cse.ohio-state.edu/~huai/)
Siyuan Ma [http://siyuan.biz/home](http://siyuan.biz/home)
