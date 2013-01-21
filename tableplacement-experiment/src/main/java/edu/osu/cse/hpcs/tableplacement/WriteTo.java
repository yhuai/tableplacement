package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.MultiFileWriter;
import edu.osu.cse.hpcs.tableplacement.multifile.RCFileMultiFileWriter;

public abstract class WriteTo {
  protected TableProperty prop;
  protected List<Column> columns;
  protected FileSystem fs;
  protected Configuration conf;
  protected Path outputDir;
  protected ColumnarSerDeBase serde;
  protected StandardStructObjectInspector rowHiveObjectInspector;
  protected int columnCount;
  protected long rowCount;
  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<ColumnFileGroup, List<Integer>> columnIdMapping;

  // Performance measures
  protected long totalRowGenerationTimeInNano;
  protected long totalRowSerializationTimeInNano;
  protected Map<String, Long> otherMeasures;

  public WriteTo(String propertyFilePath, String outputPath,
      long rowCount, Properties cmdProperties, Logger log) throws IOException,
      TablePropertyException, SerDeException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    prop = new TableProperty(new File(propertyFilePath), cmdProperties);
    prop.prepareColumns();

    columns = prop.getColumnList();
    columnCount = columns.size();
    columnFileGroups = prop.getColumnFileGroups();
    columnIdMapping = new HashMap<ColumnFileGroup, List<Integer>>();
    int count = 0;
    for (ColumnFileGroup group: columnFileGroups) {
      List<Integer> ids = new ArrayList<Integer>();
      for (Column column: group.getColumns()) {
        ids.add(count);
        count++;
      }
      columnIdMapping.put(group, ids);
    }
    
    conf = new Configuration();
    prop.copyToHadoopConf(conf);
    //fs = FileSystem.getLocal(conf);
    outputDir = new Path(outputPath);
    fs = outputDir.getFileSystem(conf);
    if (fs.exists(outputDir)) {
      log.info(outputDir.getName() + " already exists in " + outputDir.getParent()
          + ". Delete it first.");
      fs.delete(outputDir, true);
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

  public abstract long write() throws IOException, SerDeException,
    InstantiationException, IllegalAccessException, ClassNotFoundException,
    TablePropertyException;

  public long doWrite(MultiFileWriter writer, Logger log)
      throws SerDeException, IOException {
    long totalSerializedDataSize = 0;
    long[] columnSerializedDataSize = new long[columnCount];
    long ts;
    assert totalRowGenerationTimeInNano == 0;
    assert totalRowSerializationTimeInNano == 0;
    long rowAppendTime = 0;

    for (long i = 0; i < rowCount; i++) {
      ts = System.nanoTime();
      Map<String, List<Object>> row = new HashMap<String, List<Object>>();
      for (ColumnFileGroup columnFileGroup: columnFileGroups) {
        List<Object> thisGroup = new ArrayList<Object>();
        for (Column column: columnFileGroup.getColumns()){
          thisGroup.add(column.nextValue());
        }
        row.put(columnFileGroup.getName(), thisGroup);
      }
      totalRowGenerationTimeInNano += System.nanoTime() - ts;
      
      ts = System.nanoTime();
      Map<String, BytesRefArrayWritable> vals =
          new HashMap<String, BytesRefArrayWritable>();
      for (ColumnFileGroup group: columnFileGroups) {
        String groupName = group.getName();
        List<Object> thisGroup = row.get(groupName);
        ColumnarSerDeBase serde = writer.getGroupSerDe(groupName);
        ObjectInspector groupOI = group.getGroupHiveObjectInspector();
        BytesRefArrayWritable bytes = (BytesRefArrayWritable) serde.serialize(
            thisGroup, groupOI);
        vals.put(groupName, bytes);
        List<Integer> columnIds = columnIdMapping.get(group);
        for (int j = 0; j < bytes.size(); j++) {
          BytesRefWritable ref = bytes.get(j);
          int length = ref.getLength();
          totalSerializedDataSize += length;
          columnSerializedDataSize[columnIds.get(j)] += length;
        }
      }
      totalRowSerializationTimeInNano += System.nanoTime() - ts;
      ts = System.nanoTime();
      writer.append(vals);
      rowAppendTime += System.nanoTime() - ts;
    }

    writer.close();
    log.info("Total serialized data size: " + totalSerializedDataSize);
    for (ColumnFileGroup group: columnFileGroups) {
      for (Integer id: columnIdMapping.get(group)) {
        log.info("Column " + id + " is stored in file for group " +
            group.getName() + ". Serialized data size: " +
            columnSerializedDataSize[id]);
      }
    }
    
    otherMeasures.put("Row append time (ms)", rowAppendTime / 1000000);

    return totalSerializedDataSize;
  }
  
  public abstract String getFormatName();

  public void runTest() throws IOException, SerDeException, InstantiationException, IllegalAccessException, ClassNotFoundException, TablePropertyException {
    System.out.println("Writing data to " + getFormatName() + " ...");
    long start = System.nanoTime();
    long totalSerializedDataSize = write();
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
