package org.apache.trevni;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;
import org.apache.trevni.InputBuffer;
import org.apache.trevni.InputBytes;
import org.apache.trevni.OutputBuffer;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.TestBase;
import edu.osu.cse.hpcs.tableplacement.TestFormatBase;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestSerDe extends TestBase {

  protected Logger log = Logger.getLogger(TestSerDe.class);

  protected TableProperty testTableProperty;
  protected Configuration hadoopConf;

  private static final int COUNT = 1000;

  public TestSerDe() throws URISyntaxException, IOException,
      TablePropertyException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testColumns.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);
    testTableProperty.prepareColumns();
    hadoopConf = new Configuration();
  }

  public void doTestColumnarSerDe(Class<?> serDeClass) throws SerDeException,
      TablePropertyException, InstantiationException, IllegalAccessException,
      IOException {

    log.info("Testing ColumnarSerDe class " + serDeClass.getCanonicalName());

    ColumnarSerDeBase serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());

    List<Column> columns = testTableProperty.getColumnList();
    int columnCount = columns.size();
    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = TestFormatBase.getTest4ColRows(COUNT, 3);
    ByteBuffer[][] buffer = new ByteBuffer[columnCount][];
    OutputBuffer[] outs = new OutputBuffer[columnCount];
    for (int i = 0; i < columnCount; i++) {
      outs[i] = new OutputBuffer();
      buffer[i] = new ByteBuffer[rows.size()];
    }
    int count = 0;
    for (List<Object> row : rows) {
      BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(row,
          rowHiveObjectInspector);
      Assert.assertEquals(columnCount, braw.size());
      for (int i = 0; i < columnCount; i++) {
        BytesRefWritable ref = braw.get(i);
        buffer[i][count] = ByteBuffer.wrap(ref.getData(), ref.getStart(),
            ref.getLength());
        outs[i].writeBytes(buffer[i][count]);
      }
      count++;
    }

    ObjectInspector out_oi = serde.getObjectInspector();
    InputBuffer[] ins = new InputBuffer[columnCount];
    for (int i = 0; i < columnCount; i++) {
      ins[i] = new InputBuffer(new InputBytes(outs[i].toByteArray()));
    }
    for (int i = 0; i < rows.size(); i++) {
      BytesRefArrayWritable braw = new BytesRefArrayWritable();
      braw.resetValid(columnCount);
      for (int j = 0; j < columnCount; j++) {
        ByteBuffer actual = ins[j].readBytes(null);
        BytesRefWritable ref = braw.get(j);
        ref.set(actual.array(), actual.arrayOffset(), actual.capacity());
      }
      Object actualRow = serde.deserialize(braw);
      Object expectedRow = rows.get(i);
      if (0 != ObjectInspectorUtils
          .compare(expectedRow, rowHiveObjectInspector, actualRow, out_oi,
              new FullMapEqualComparer())) {
        System.out.println("expected = "
            + SerDeUtils.getJSONString(expectedRow, rowHiveObjectInspector));
        System.out.println("actual = "
            + SerDeUtils.getJSONString(actualRow, out_oi));
        Assert.fail("Deserialized object does not compare");
      }
    }

  }

  @Test
  public void testColumnarSerDe() throws SerDeException,
      TablePropertyException, InstantiationException, IllegalAccessException,
      IOException {
    doTestColumnarSerDe(ColumnarSerDe.class);
    doTestColumnarSerDe(LazyBinaryColumnarSerDe.class);
  }

}
