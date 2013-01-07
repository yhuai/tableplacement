package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestColumns extends TestBase {

  protected Logger log = Logger.getLogger(TestColumns.class);

  protected TableProperty testTableProperty;
  protected Configuration hadoopConf;

  public TestColumns() throws URISyntaxException, IOException,
      TablePropertyException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testColumns.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);
    hadoopConf = new Configuration();
  }

  @Test
  public void testHiveRowObjectInspector() throws TablePropertyException {
    List<Column> columns = testTableProperty.getColumnList();
    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();
    int columnCount = columns.size();
    Assert.assertEquals(5, columnCount);

    List<Object> row = new ArrayList<Object>(columnCount);
    row.add(1);
    row.add(2.2);
    row.add("333");
    
    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("4444", 44444);
    row.add(map);

    List<Object> struct = new ArrayList<Object>();
    struct.add("struct1");
    struct.add(10);
    struct.add(22.2);
    row.add(struct);
    
    Map<String, Integer> mapRef = new HashMap<String, Integer>();
    mapRef.put("4444", 44444);
    List<Object> structRef = new ArrayList<Object>();
    structRef.add("struct1");
    structRef.add(10);
    structRef.add(22.2);

    List<? extends StructField> fields = rowHiveObjectInspector
        .getAllStructFieldRefs();
    Assert.assertEquals(5, fields.size());
    Assert.assertEquals(1,
        rowHiveObjectInspector.getStructFieldData(row, fields.get(0)));
    Assert.assertEquals(2.2,
        rowHiveObjectInspector.getStructFieldData(row, fields.get(1)));
    Assert.assertEquals("333",
        rowHiveObjectInspector.getStructFieldData(row, fields.get(2)));
    Assert.assertEquals(mapRef,
        rowHiveObjectInspector.getStructFieldData(row, fields.get(3)));
    Assert.assertEquals(structRef,
        rowHiveObjectInspector.getStructFieldData(row, fields.get(4)));
  }

  public void doTestColumnarSerDe(Class<?> serDeClass) throws SerDeException,
      TablePropertyException, InstantiationException, IllegalAccessException {

    log.info("Testing ColumnarSerDe class " + serDeClass.getCanonicalName());

    ColumnarSerDeBase serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());

    List<Column> columns = testTableProperty.getColumnList();
    int columnCount = columns.size();
    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<Object> row = new ArrayList<Object>(columnCount);
    row.add(128);
    row.add(2.2);
    row.add("333");

    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("4444", 44444);
    row.add(map);

    List<Object> struct = new ArrayList<Object>();
    struct.add("struct1");
    struct.add(10);
    struct.add(22.2);
    row.add(struct);

    BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(row,
        rowHiveObjectInspector);
    Assert.assertEquals(columnCount, braw.size());
    for (int i = 0; i < columnCount; i++) {
      log.info("Size of Column " + i + ": " + braw.get(i).getLength());
    }
    ObjectInspector out_oi = serde.getObjectInspector();
    Object out_o = serde.deserialize(braw);

    if (0 != ObjectInspectorUtils.compare(row, rowHiveObjectInspector, out_o,
        out_oi, new FullMapEqualComparer())) {
      System.out.println("expected = "
          + SerDeUtils.getJSONString(row, rowHiveObjectInspector));
      System.out.println("actual = " + SerDeUtils.getJSONString(out_o, out_oi));
      Assert.fail("Deserialized object does not compare");
    }
  }

  @Test
  public void testColumnarSerDe() throws SerDeException,
      TablePropertyException, InstantiationException, IllegalAccessException {
    doTestColumnarSerDe(ColumnarSerDe.class);
    doTestColumnarSerDe(LazyBinaryColumnarSerDe.class);
  }

}
