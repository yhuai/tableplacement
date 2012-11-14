package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.util.CmdTool;

public class ReadRCFileFromLocal {

  protected Logger log = Logger.getLogger(ReadRCFileFromLocal.class);

  private TableProperty prop;
  private List<Column> columns;
  private FileSystem localFS;
  private Configuration conf;
  private Path file;
  private ColumnarSerDeBase serde;
  private StandardStructObjectInspector rowHiveObjectInspector;
  private final int columnCount;
  private final String readColumnStr;
  private ArrayList<Integer> readCols;

  public ReadRCFileFromLocal(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath), cmdProperties);
    columns = prop.getColumns();
    columnCount = columns.size();
    conf = new Configuration();
    prop.copyToHadoopConf(conf);
    localFS = FileSystem.getLocal(conf);
    file = new Path(inputPath);
    String serDeClassName = prop.get(TableProperty.SERDE_CLASS);
    if (serDeClassName == null) {
      serDeClassName = TableProperty.DEFAULT_SERDE_CLASS;
    }
    Class serDeClass = Class.forName(serDeClassName);
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(conf, prop.getProperties());
    rowHiveObjectInspector = (StandardStructObjectInspector) prop
        .getHiveRowObjectInspector();

    readColumnStr = prop.get(TableProperty.READ_COLUMN_STR);
    if (readColumnStr == null || TableProperty.READ_ALL_COLUMNS_STR.equals(readColumnStr)) {
      ColumnProjectionUtils.setFullyReadColumns(conf);
    } else {
      readCols = CmdTool.parseReadColumnStr(readColumnStr);
      if (Collections.max(readCols) > columnCount - 1) {
        log.error("Invilid " + TableProperty.READ_COLUMN_STR + " "
            + readColumnStr + ". There is only " + columnCount + " columns");
        throw new TablePropertyException("Invilid "
            + TableProperty.READ_COLUMN_STR + " " + readColumnStr
            + ". There is only " + columnCount + " columns");
      }
      ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    }
    
    prop.dump();
  }

  public long doRead() throws IOException, SerDeException {
    RCFile.Reader reader = new RCFile.Reader(localFS, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
    long rowCount = 0;
    long totalSerializedDataSize = 0;

    while (reader.next(rowID)) {
      for (int j = 0; j < braw.size(); j++) {
        totalSerializedDataSize += braw.get(j).getLength();
      }
      reader.getCurrentRow(braw);
      Object row = serde.deserialize(braw);
      rowCount++;
    }
    reader.close();
    log.info("Row count : " + rowCount);
    log.info("Total serialized data size: " + totalSerializedDataSize);
    return totalSerializedDataSize;
  }

  public static void main(String[] args) throws Exception {

    Properties cmdProperties = CmdTool.inputParameterParser(args);
    String propertyFilePath = cmdProperties
        .getProperty(CmdTool.TABLE_PROPERTY_FILE);
    String inputPathStr = cmdProperties.getProperty(CmdTool.INPUT_FILE);

    if (propertyFilePath == null || inputPathStr == null) {
      System.out.println("usage: " + ReadRCFileFromLocal.class.getName()
          + " -t <table property file> -i <input> [-p ...]");
      System.out.println("You can overwrite properties defined in the "
          + "table property file through '-p key value'");
      System.exit(-1);
    }

    System.out.println("Table property file: " + propertyFilePath);
    System.out.println("Input file: " + inputPathStr);
    System.out.println("Reading data from RCFile ...");
    ReadRCFileFromLocal readRCFileLocal = new ReadRCFileFromLocal(propertyFilePath,
        inputPathStr, cmdProperties);
    long start = System.nanoTime();
    long totalSerializedDataSize = readRCFileLocal.doRead();
    long end = System.nanoTime();
    System.out.println("Reading from RCFile finished.");
    System.out
        .println("Total serialized data size: " + totalSerializedDataSize);
    System.out.println("Elapsed time: " + (end - start) / 1000000 + " ms");
    System.out.println("Throughput MB/s: " +
        totalSerializedDataSize * 1.0 / 1024 / 1024 / (end - start) * 1000000000);
  }
}
