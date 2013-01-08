package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public abstract class WriteToLocal {
  protected TableProperty prop;
  protected List<Column> columns;
  protected FileSystem localFS;
  protected Configuration conf;
  protected Path file;
  protected ColumnarSerDeBase serde;
  protected StandardStructObjectInspector rowHiveObjectInspector;
  protected int columnCount;
  protected long rowCount;

  // Performance measures
  protected long totalRowGenerationTimeInNano;
  protected long totalRowSerializationTimeInNano;
  protected Map<String, Long> otherMeasures;

  public WriteToLocal(String propertyFilePath, String outputPath,
      long rowCount, Properties cmdProperties, Logger log) throws IOException,
      TablePropertyException, SerDeException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath), cmdProperties);
    prop.prepareColumns();

    columns = prop.getColumnList();
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
    rowHiveObjectInspector = (StandardStructObjectInspector) prop
        .getHiveRowObjectInspector();

    this.rowCount = rowCount;
    this.totalRowGenerationTimeInNano = 0;
    this.totalRowSerializationTimeInNano = 0;
    this.otherMeasures = new HashMap<String, Long>();
    
    prop.dump();
  }

  public abstract long doWrite() throws IOException, SerDeException;

  public abstract String getFormatName();

  public void runTest() throws IOException, SerDeException {
    System.out.println("Writing data to " + getFormatName() + " ...");
    long start = System.nanoTime();
    long totalSerializedDataSize = doWrite();
    long end = System.nanoTime();
    System.out.println("Writing to " + getFormatName() + " finished.");
    System.out.println("Total serialized data size (MiB): "
        + totalSerializedDataSize * 1.0 / 1024 / 1024);
    System.out.println("Total elapsed time: " + (end - start) * 1.0 / 1000000
        + " ms");
    System.out.println("Total row generation time: "
        + totalRowGenerationTimeInNano * 1.0 / 1000000 + " ms");
    System.out.println("Average row generation time: "
        + totalRowGenerationTimeInNano * 1.0 / 1000000 / rowCount + " ms");
    System.out.println("Total row serialization time: "
        + totalRowSerializationTimeInNano * 1.0 / 1000000 + " ms");
    System.out.println("Average row serialization time: "
        + totalRowSerializationTimeInNano * 1.0 / 1000000 / rowCount + " ms");
    System.out.println("Throughput (MiB/s): " + totalSerializedDataSize * 1.0
        / 1024 / 1024 / (end - start) * 1000000000);
    for (Entry<String, Long> entry: otherMeasures.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    
  }
}
