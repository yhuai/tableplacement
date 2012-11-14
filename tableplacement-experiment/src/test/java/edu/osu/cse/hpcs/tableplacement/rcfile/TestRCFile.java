package edu.osu.cse.hpcs.tableplacement.rcfile;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.TestBase;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestRCFile extends TestBase {

  protected Logger log = Logger.getLogger(TestRCFile.class);

  protected TableProperty testTableProperty;
  protected Configuration hadoopConf;
  protected FileSystem localFS;
  protected Path file;
  protected ColumnarSerDeBase serde;

  protected List<Column> columns;
  protected final int columnCount;

  public TestRCFile() throws URISyntaxException, TablePropertyException,
      IOException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testColumns.properties");
    testTableProperty = new TableProperty(new File(url.toURI()));
    columns = testTableProperty.getColumns();
    columnCount = columns.size();
    hadoopConf = new Configuration();
    testTableProperty.copyToHadoopConf(hadoopConf);
    localFS = FileSystem.getLocal(hadoopConf);
    file = new Path(resourceDir, "testRCFile");
    if (localFS.exists(file)) {
      log.info(file.getName() + " already exists in " + file.getParent()
          + ". Delete it first.");
      localFS.delete(file, true);
    }

  }

  private List<List<Object>> getTestRows(final int rowCount, final int mapSize) {
    List<List<Object>> ret = new ArrayList<List<Object>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      Map<String, Integer> map = new HashMap<String, Integer>();
      for (int j = 0; j < mapSize; j++) {
        map.put("r" + i + "m" + j, i * 10 + j);
      }
      List<Object> row = new ArrayList<Object>(4);
      row.add(i * 100);
      row.add(i * 100 + i * 0.001);
      row.add("r" + i);
      row.add(map);

      ret.add(row);
    }

    return ret;
  }

  private void doRCFileTest(Class<?> serDeClass) throws SerDeException,
      InstantiationException, IllegalAccessException, IOException {
    log.info("Testing RCFile writer and reader with ColumnarSerDe class "
        + serDeClass.getCanonicalName());
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());
    RCFileOutputFormat.setColumnNumber(hadoopConf, columnCount);

    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = getTestRows(10, 3);
    log.info("Writing RCFile ...");
    int totalSerializedDataSize = 0;
    RCFile.Writer writer = new RCFile.Writer(localFS, hadoopConf, file, null,
        null);
    for (int i = 0; i < rows.size(); i++) {
      BytesRefArrayWritable bytes = (BytesRefArrayWritable) serde.serialize(
          rows.get(i), rowHiveObjectInspector);
      for (int j=0; j<bytes.size(); j++) {
        totalSerializedDataSize += bytes.get(j).getLength();
      }
      writer.append(bytes);
    }
    writer.close();
    log.info("Total serialized data size: " + totalSerializedDataSize);

    log.info("Reading RCFile ...");
    ObjectInspector out_oi = serde.getObjectInspector();
    RCFile.Reader reader = new RCFile.Reader(localFS, file, hadoopConf);
    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable braw = new BytesRefArrayWritable(4);
    int indx = 0;
    while (reader.next(rowID)) {
      reader.getCurrentRow(braw);
      Object actualRow = serde.deserialize(braw);
      Object expectedRow = rows.get(indx);
      if (0 != ObjectInspectorUtils.compare(expectedRow, rowHiveObjectInspector, actualRow,
          out_oi, new FullMapEqualComparer())) {
        System.out.println("expected = "
            + SerDeUtils.getJSONString(expectedRow, rowHiveObjectInspector));
        System.out.println("actual = " + SerDeUtils.getJSONString(actualRow, out_oi));
        Assert.fail("Deserialized object does not compare");
      }
      indx++;
    }
    reader.close();
    log.info("Done");
  }

  @Test
  public void testRCFile() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException {
    doRCFileTest(ColumnarSerDe.class);
    doRCFileTest(LazyBinaryColumnarSerDe.class);
  }

}
