package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.TestFormatBase;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestRCFileMultiFile extends TestMultiFile {

  protected static Logger log = Logger.getLogger(TestRCFileMultiFile.class);
  
  public TestRCFileMultiFile() throws URISyntaxException, IOException,
      TablePropertyException {
    super(log);
  }

  public List<Map<String, List<Object>>> writeData(Configuration conf)
      throws IOException, SerDeException, InstantiationException, IllegalAccessException, ClassNotFoundException, TablePropertyException {
    log.info("Write RCFile ...");
    RCFileMultiFileWriter writer = new RCFileMultiFileWriter(conf, path);
    
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
  
  public void doRCFileFullMultiFileReadTest(Class<?> serDeClass)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException, ClassNotFoundException, TablePropertyException {
    log.info("RCFileFullMultiFileReadTest");
    testTableProperty.set(TableProperty.SERDE_CLASS, serDeClass.getCanonicalName());
    Configuration conf = new Configuration();
    testTableProperty.copyToHadoopConf(conf);
    
    List<Map<String, List<Object>>> rows = writeData(conf);
    
    log.info("Reading RCFile ...");
    Map<String, List<Integer>> readColumns =
        MultiFileReader.parseReadColumnMultiFileStr(fullReadColumnStr);
    log.info("Read columns: " + readColumns.toString());
    RCFileMultiFileReader reader = new RCFileMultiFileReader(conf, path, readColumns);
    LongWritable rowID = new LongWritable();
    int indx = 0;
    Map<String, BytesRefArrayWritable> ret = new HashMap<String, BytesRefArrayWritable>();
    for (Entry<String, List<Integer>> entry: readColumns.entrySet()) {
      ret.put(entry.getKey(), new BytesRefArrayWritable(entry.getValue().size()));
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
  
  public void doRCFilePartialMultiFileReadTest(Class<?> serDeClass)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException, ClassNotFoundException, TablePropertyException {
    log.info("RCFilePartialMultiFileReadTest");
    testTableProperty.set(TableProperty.SERDE_CLASS, serDeClass.getCanonicalName());
    Configuration conf = new Configuration();
    testTableProperty.copyToHadoopConf(conf);
    
    List<Map<String, List<Object>>> rows = writeData(conf);
    
    log.info("Reading RCFile ...");
    for (int i=0; i<partialReadColumnStrs.length; i++) {
      String readColumnStr = partialReadColumnStrs[i];
      Map<String, List<Integer>> readColumns =
          MultiFileReader.parseReadColumnMultiFileStr(readColumnStr);
      log.info("Read columns: " + readColumns.toString());
      RCFileMultiFileReader reader = new RCFileMultiFileReader(conf, path, readColumns);
      LongWritable rowID = new LongWritable();
      int indx = 0;
      Map<String, BytesRefArrayWritable> ret = new HashMap<String, BytesRefArrayWritable>();
      for (Entry<String, List<Integer>> entry: readColumns.entrySet()) {
        ret.put(entry.getKey(), new BytesRefArrayWritable(entry.getValue().size()));
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
    }
    log.info("Done");
  }
  
  @Test
  public void testRCFileMultiFile() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException, ClassNotFoundException, TablePropertyException {
    doRCFileFullMultiFileReadTest(ColumnarSerDe.class);
    doRCFileFullMultiFileReadTest(LazyBinaryColumnarSerDe.class);

    doRCFilePartialMultiFileReadTest(ColumnarSerDe.class);
    doRCFilePartialMultiFileReadTest(LazyBinaryColumnarSerDe.class);
  }
}
