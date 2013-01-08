package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.util.CmdTool;

public abstract class ReadFromLocal {
  protected TableProperty prop;
  protected List<Column> columns;
  protected FileSystem localFS;
  protected Configuration conf;
  protected Path file;
  protected ColumnarSerDeBase serde;
  protected StandardStructObjectInspector rowHiveObjectInspector;
  protected int columnCount;
  protected String readColumnStr;
  protected ArrayList<Integer> readCols;
  protected long rowCount;
  
  //Performance measures
 protected long totalRowReadTimeInNano;
 protected long totalRowDeserializationTimeInNano;
  
  public ReadFromLocal(String propertyFilePath, String inputPath,
      Properties cmdProperties, Logger log) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath), cmdProperties);
    prop.prepareColumns();
    columns = prop.getColumnList();
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
    rowHiveObjectInspector = (StandardStructObjectInspector) prop
        .getHiveRowObjectInspector();

    readColumnStr = prop.get(TableProperty.READ_COLUMN_STR);
    if (readColumnStr == null
        || TableProperty.READ_ALL_COLUMNS_STR.equals(readColumnStr)) {
      readCols = null;
    } else {
      readCols = CmdTool.parseReadColumnStr(readColumnStr);
      if (Collections.max(readCols) > columnCount - 1) {
        log.error("Invilid " + TableProperty.READ_COLUMN_STR + " "
            + readColumnStr + ". There is only " + columnCount + " columns");
        throw new TablePropertyException("Invilid "
            + TableProperty.READ_COLUMN_STR + " " + readColumnStr
            + ". There is only " + columnCount + " columns");
      }
    }

    if (readCols == null) {
      ColumnProjectionUtils.setFullyReadColumns(conf);
    } else {
      ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    }
    // initialization must be done after read columns have been
    // set in hadoop conf
    serde.initialize(conf, prop.getProperties());
    
    prop.dump();
  }
  
  public abstract long doRead() throws IOException, SerDeException;

  public abstract String getFormatName();
  
  public void runTest() throws IOException, SerDeException {
    System.out.println("Reading data from " + getFormatName() + " ...");
    long start = System.nanoTime();
    long totalSerializedDataSize = doRead();
    long end = System.nanoTime();
    System.out.println("Reading from " + getFormatName() + " finished.");
    System.out
        .println("Total serialized data size (MiB): " + totalSerializedDataSize * 1.0 / 1024 / 1024);
    System.out.println("Total elapsed time: " + (end - start) / 1000000 + " ms");
    System.out.println("Total row read time: "
        + totalRowReadTimeInNano * 1.0 / 1000000 + " ms");
    System.out.println("Average row read time: "
        + totalRowReadTimeInNano * 1.0 / 1000000 / rowCount + " ms");
    System.out.println("Total row deserialization time: "
        + totalRowDeserializationTimeInNano * 1.0 / 1000000 + " ms");
    System.out.println("Average row deserialization time: "
        + totalRowDeserializationTimeInNano * 1.0 / 1000000 / rowCount + " ms");
    System.out.println("Throughput MiB/s: " + totalSerializedDataSize * 1.0
        / 1024 / 1024 / (end - start) * 1000000000);
  }
}
