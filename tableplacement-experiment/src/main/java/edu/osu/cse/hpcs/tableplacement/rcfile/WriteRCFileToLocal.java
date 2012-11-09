package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;


// TODO: need rework on this. 
// See org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory and 
// tests like org.apache.hadoop.hive.serde2.objectinspector.TestStandardObjectInspectors
// on how to use serde.
public class WriteRCFileToLocal {

  Logger log = Logger.getLogger(WriteRCFileToLocal.class);

  TableProperty prop;
  List<Column> columns;
  FileSystem localFS;
  Configuration conf;
  Path[] files;
  ColumnarSerDeBase serde;
  final int columnNum;
  final int threadNum;

  public WriteRCFileToLocal(String propertyFilePath, String outputPath, int threadNum)
      throws IOException, TablePropertyException, SerDeException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath));
    columns = prop.getColumns();
    columnNum = columns.size();
    conf = new Configuration();
    prop.copyToHadoopConf(conf);
    localFS = FileSystem.getLocal(conf);
    this.threadNum = threadNum;
    files = new Path[threadNum];
    for (int i=0; i<files.length; i++) {
      files[i] = new Path(outputPath, "rcfile." + i);
      if (localFS.exists(files[i])) {
        log.info(files[i].getName() + " already exists in " + outputPath + ". Delete it first.");
        localFS.delete(files[i], true);
      }
    }
    String serDeClassName = prop.get(TableProperty.SERDE_CLASS);
    if (serDeClassName == null) {
      serDeClassName = TableProperty.DEFAULT_SERDE_CLASS;
    }
    Class serDeClass = Class.forName(serDeClassName);
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(conf, prop.getProperties());
    RCFileOutputFormat.setColumnNumber(conf, columnNum);
    
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("usage: " + WriteRCFileToLocal.class.getName()
          + " <table property file> <output dir> <num of threads>");
      System.exit(-1);
    }

    String propertyFilePath = args[0];
    String outputPathStr = args[1];
    int threadNum = Integer.valueOf(args[2]);
    System.out.println("Using table property file " + propertyFilePath);
    System.out.println("Generating RCFiles to " + outputPathStr + " with " + threadNum + " threads");

  }
}
