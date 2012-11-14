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
import edu.osu.cse.hpcs.tableplacement.TestFormatBase;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestRCFile extends TestFormatBase {

  protected static Logger log = Logger.getLogger(TestRCFile.class);

  public TestRCFile() throws URISyntaxException, TablePropertyException,
      IOException {
    super("testColumns.properties", "testRCFile", log);
  }

  private void doRCFileFullReadTest(Class<?> serDeClass) throws SerDeException,
      InstantiationException, IllegalAccessException, IOException {
    log.info("Testing RCFile writer and reader with ColumnarSerDe class "
        + serDeClass.getCanonicalName());
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());
    RCFileOutputFormat.setColumnNumber(hadoopConf, columnCount);

    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = getTest4ColRows(10, 3);
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
    doRCFileFullReadTest(ColumnarSerDe.class);
    doRCFileFullReadTest(LazyBinaryColumnarSerDe.class);
  }

}
