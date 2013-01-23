package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.BaseFormatTestClass;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class MultiFileTestClass extends BaseFormatTestClass {

  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<String, StandardStructObjectInspector> groupOI;
  
  protected final int rowCount = 1000;
  
  protected static final String fullReadColumnStr =
      "cfg1:all|cfg2:all|cfg3:all";
  protected static final String[] partialReadColumnStrs =
    {"cfg1:0", "cfg2:1", "cfg3:0", "cfg1:0|cfg2:0|cfg3:0"};

  public MultiFileTestClass(Class<?> serdeClass, String dirName, Logger log) throws URISyntaxException, IOException,
    TablePropertyException {
    super("testColumns.properties", dirName, log);
    testTableProperty.set(TableProperty.COLUMN_FILE_GROUP,
        "cfg1:cdouble,cint|cfg2:cstring,cstruct1|cfg3:cmap1");
    testTableProperty.set(TableProperty.SERDE_CLASS, serdeClass.getCanonicalName());
    testTableProperty.copyToHadoopConf(hadoopConf);
    testTableProperty.prepareColumns();
    columnFileGroups = testTableProperty.getColumnFileGroups();
    groupOI = new HashMap<String, StandardStructObjectInspector>();
    for (ColumnFileGroup group: columnFileGroups) {
      groupOI.put(group.getName(), 
          (StandardStructObjectInspector)group.getGroupHiveObjectInspector());
    }
    localFS.mkdirs(path);
  }
  
  public List<Map<String, List<Object>>> genData(int rowCount) {
    List<Map<String, List<Object>>> ret =
        new ArrayList<Map<String, List<Object>>>();
    for (int i=0; i<rowCount; i++) {
      Map<String, List<Object>> row = new HashMap<String, List<Object>>();
      for (ColumnFileGroup columnFileGroup: columnFileGroups) {
        List<Object> thisGroup = new ArrayList<Object>();
        for (Column column: columnFileGroup.getColumns()){
          thisGroup.add(column.nextValue());
        }
        row.put(columnFileGroup.getName(), thisGroup);
      }
      ret.add(row);
    }
    return ret;
  }
  
  public RCFileMultiFileWriter getRCFileWriter()
      throws IOException, InstantiationException, IllegalAccessException,
      SerDeException, ClassNotFoundException, TablePropertyException {
    return new RCFileMultiFileWriter(hadoopConf, path);
  }
  
  public TrevniMultiFileWriter getTrevniWriter()
      throws IOException, InstantiationException, IllegalAccessException,
      SerDeException, ClassNotFoundException, TablePropertyException {
    return new TrevniMultiFileWriter(hadoopConf, path);
  }

  public RCFileMultiFileReader getRCFileReader(String readColumnsStr)
      throws IOException, InstantiationException, IllegalAccessException,
      SerDeException, ClassNotFoundException, TablePropertyException {
    return new RCFileMultiFileReader(hadoopConf, path, readColumnsStr);
  }
  
  public TrevniMultiFileReader getTrevniReader(String readColumnsStr)
      throws IOException, InstantiationException, IllegalAccessException,
      SerDeException, ClassNotFoundException, TablePropertyException {
    return new TrevniMultiFileReader(hadoopConf, path, readColumnsStr, true);
  }

  public List<Map<String, List<Object>>> writeData(
      MultiFileWriter writer)
      throws IOException, SerDeException, InstantiationException, IllegalAccessException, ClassNotFoundException, TablePropertyException {
    log.info("Write test file using " + writer.getClass().getCanonicalName());

    List<Map<String, List<Object>>> rows = genData(rowCount);
    for (Map<String, List<Object>> row: rows) {
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
      }
      writer.append(vals);
    }
    writer.close();
    return rows;
  }
  
  public void doFullMultiFileReadTest(
      List<Map<String, List<Object>>> rows,
      MultiFileReader reader,
      String testName)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException, ClassNotFoundException, TablePropertyException {
    log.info("[doFullMultiFileReadTest]" + testName);
    
    log.info("Reading test file using " + reader.getClass().getCanonicalName());
    Map<String, List<Integer>> readColumns = reader.getReadColumns();
    log.info("Read columns: " + readColumns.toString());
    LongWritable rowID = new LongWritable();
    int indx = 0;
    Map<String, BytesRefArrayWritable> ret = new HashMap<String, BytesRefArrayWritable>();
    List<ColumnFileGroup> groups = reader.getColumnFileGroups();
    for (ColumnFileGroup group: groups) {
      BytesRefArrayWritable braw = new BytesRefArrayWritable(group.getColumns().size());
      braw.resetValid(group.getColumns().size());
      ret.put(group.getName(), braw);
    }
    while(reader.next(rowID)) {
      reader.getCurrentRow(ret);
      for (Entry<String, BytesRefArrayWritable> groupRet: ret.entrySet()) {
        String groupName = groupRet.getKey();
        ColumnarSerDeBase serde = reader.getGroupSerDe(groupName);
        Object actualRow = serde.deserialize(groupRet.getValue());        
        Object expectedRow = rows.get(indx).get(groupName);      
        
        if (0 != ObjectInspectorUtils
            .compare(expectedRow, 
                groupOI.get(groupName), 
                actualRow, 
                serde.getObjectInspector(),
                new FullMapEqualComparer())) {
          System.out.println("expected = "
              + SerDeUtils.getJSONString(expectedRow, groupOI.get(groupName)));
          System.out.println("actual = "
              + SerDeUtils.getJSONString(actualRow, serde.getObjectInspector()));
          Assert.fail("Deserialized object does not compare");
        }
      }
      indx++;
    }
    log.info("Done");
  }
  
  public void doPartialMultiFileReadTest(
      List<Map<String, List<Object>>> rows,
      MultiFileReader reader,
      String testName)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException, ClassNotFoundException, TablePropertyException {
    log.info("[doPartialMultiFileReadTest]" + testName);
    
    log.info("Reading test file using " + reader.getClass().getCanonicalName());
    Map<String, List<Integer>> readColumns = reader.getReadColumns();
    log.info("Read columns: " + readColumns.toString());
    LongWritable rowID = new LongWritable();
    int indx = 0;
    Map<String, BytesRefArrayWritable> ret = new HashMap<String, BytesRefArrayWritable>();
    List<ColumnFileGroup> groups = reader.getColumnFileGroups();
    for (ColumnFileGroup group: groups) {
      if (!readColumns.keySet().contains(group.getName())) {
        continue;
      }
      BytesRefArrayWritable braw = new BytesRefArrayWritable(group.getColumns().size());
      braw.resetValid(group.getColumns().size());
      ret.put(group.getName(), braw);
    }
    while(reader.next(rowID)) {
      reader.getCurrentRow(ret);
      for (Entry<String, BytesRefArrayWritable> groupRet: ret.entrySet()) {
        String groupName = groupRet.getKey();
        ColumnarSerDeBase serde = reader.getGroupSerDe(groupName);
        Object actualRow = serde.deserialize(groupRet.getValue());        
        List<Object> expectedRow = rows.get(indx).get(groupName);        
        StructObjectInspector oi = (StructObjectInspector) serde.getObjectInspector();
        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
        for (Integer col: readColumns.get(groupName)) {
          Object fieldData = oi.getStructFieldData(actualRow, fieldRefs.get(col));
          Object javaObjectData = 
              ObjectInspectorUtils.copyToStandardJavaObject(fieldData,
                  fieldRefs.get(col).getFieldObjectInspector());
          Assert.assertEquals(expectedRow.get(col), javaObjectData);
        }
      }
      indx++;
    }
    log.info("Done");
  }
}
