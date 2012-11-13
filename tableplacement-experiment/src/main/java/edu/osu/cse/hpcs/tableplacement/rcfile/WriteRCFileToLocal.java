package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class WriteRCFileToLocal {

  Logger log = Logger.getLogger(WriteRCFileToLocal.class);

  TableProperty prop;
  List<Column> columns;
  FileSystem localFS;
  Configuration conf;
  Path file;
  ColumnarSerDeBase serde;
  StandardStructObjectInspector rowHiveObjectInspector;
  final int columnCount;
  final int rowCount;

  public WriteRCFileToLocal(String propertyFilePath, String outputPath, int rowCount)
      throws IOException, TablePropertyException, SerDeException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath));
    columns = prop.getColumns();
    columnCount = columns.size();
    conf = new Configuration();
    prop.copyToHadoopConf(conf);
    localFS = FileSystem.getLocal(conf);
    file = new Path(outputPath);
    if (localFS.exists(file)) {
      log.info(file.getName() + " already exists in " + file.getParent()
          + ". Delete it first.");
      localFS.delete(file, true);
    }
    String serDeClassName = prop.get(TableProperty.SERDE_CLASS);
    if (serDeClassName == null) {
      serDeClassName = TableProperty.DEFAULT_SERDE_CLASS;
    }
    Class serDeClass = Class.forName(serDeClassName);
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(conf, prop.getProperties());
    RCFileOutputFormat.setColumnNumber(conf, columnCount);
    rowHiveObjectInspector = (StandardStructObjectInspector) prop
        .getHiveRowObjectInspector();

    this.rowCount = rowCount;
  }

  public long doWrite() throws IOException, SerDeException {
    RCFile.Writer writer = new RCFile.Writer(localFS, conf, file, null,
        null);
    
    long totalSerializedDataSize = 0;
    for (int i=0; i<rowCount; i++) {
      List<Object> row = new ArrayList<Object>(columnCount);
      for (Column col: columns) {
        row.add(col.nextValue());
      }
      BytesRefArrayWritable bytes = (BytesRefArrayWritable) serde.serialize(
          row, rowHiveObjectInspector);
      for (int j=0; j<bytes.size(); j++) {
        totalSerializedDataSize += bytes.get(j).getLength();
      }
      writer.append(bytes);
    }
    
    writer.close();
    log.info("Total serialized data size: " + totalSerializedDataSize);
    return totalSerializedDataSize;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("usage: " + WriteRCFileToLocal.class.getName()
          + " <table property file> <output> <rowCount>");
      System.exit(-1);
    }
    
    String propertyFilePath = args[0];
    String outputPathStr = args[1];
    int rowCount = Integer.valueOf(args[2]);
    System.out.println("Table property file: " + propertyFilePath);
    System.out.println("Output file: " + outputPathStr);
    System.out.println("Total number of rows: " + rowCount);
    System.out.println("Writing data to RCFile ");
    WriteRCFileToLocal writeRCFileLocal =
        new WriteRCFileToLocal(propertyFilePath, outputPathStr, rowCount);
    long start = System.nanoTime();
    long totalSerializedDataSize = writeRCFileLocal.doWrite();
    long end = System.nanoTime();
    System.out.println("Writing to RCFile finished.");
    System.out.println("Total serialized data size: " + totalSerializedDataSize);
    System.out.println("Elapsed time: " + (end - start)/1000000 + " ms");
    
  }
}
