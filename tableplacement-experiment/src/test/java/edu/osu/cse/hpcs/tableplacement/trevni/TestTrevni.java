package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.TestUtil;
import org.apache.trevni.avro.HadoopInput;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.osu.cse.hpcs.tableplacement.TestFormatBase;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

@RunWith(value = Parameterized.class)
public class TestTrevni extends TestFormatBase {

  protected static Logger log = Logger.getLogger(TestTrevni.class);

  private String codec;
  private String checksum;
  private final int rowCount = 1000;

  @Parameters
  public static Collection<Object[]> codecs() {
    Object[][] data = new Object[][] { { "null", "null" } };
    return Arrays.asList(data);
  }

  public TestTrevni(String codec, String checksum) throws URISyntaxException,
      IOException, TablePropertyException {
    super("testColumns.properties", "testTrevni", log);
    this.codec = codec;
    this.checksum = checksum;
  }

  private void doTrevniFullReadTest(Class<?> serDeClass, Class<?> inputClass)
      throws InstantiationException, IllegalAccessException, SerDeException,
      IOException {

    log.info("Testing Trevni write and read with ColumnarSerDe class "
        + serDeClass.getCanonicalName() + " and InputClass " + inputClass.getCanonicalName());
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());

    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = getTest4ColRows(rowCount, 3);
    log.info("Writing Trevni ...");
    int totalSerializedDataSize = 0;
    ColumnFileWriter out = new ColumnFileWriter(
        WriteTrevniToLocal.createFileMeta(codec, checksum),
        WriteTrevniToLocal.createColumnMetaData(columns, columnCount));
    FSDataOutputStream trevniOutputStream = localFS.create(file);

    for (int i = 0; i < rows.size(); i++) {
      BytesRefArrayWritable bytes = (BytesRefArrayWritable) serde.serialize(
          rows.get(i), rowHiveObjectInspector);
      ByteBuffer[] row = new ByteBuffer[bytes.size()];
      for (int j = 0; j < bytes.size(); j++) {
        totalSerializedDataSize += bytes.get(j).getLength();
        BytesRefWritable ref = bytes.get(j);
        row[j] = ByteBuffer
            .wrap(ref.getData(), ref.getStart(), ref.getLength());
      }
      out.writeRow((Object[]) row);
    }
    out.writeTo(trevniOutputStream);
    trevniOutputStream.close();
    log.info("Total serialized data size: " + totalSerializedDataSize);

    log.info("Reading Trevni ...");
    ObjectInspector out_oi = serde.getObjectInspector();
    log.info("FileSystem: " + file.getFileSystem(hadoopConf).getClass());
    assert file.getFileSystem(hadoopConf) instanceof LocalFileSystem;

    ColumnFileReader in;
    if (HadoopInput2.class.equals(inputClass)) {
      in = new ColumnFileReader(
          new HadoopInput2(file, hadoopConf));
    } else {
      in = new ColumnFileReader(
          new HadoopInput(file, hadoopConf));
    }
    
    ColumnMetaData[] metadata = in.getColumnMetaData();
    for (int i = 0; i < metadata.length; i++) {
      log.info(metadata[i].getName() + " " + metadata[i].getType() + " "
          + metadata[i].getNumber());
    }
    Assert.assertEquals(rowCount, in.getRowCount());
    Assert.assertEquals(columnCount, in.getColumnCount());

    TrevniRowReader reader = new TrevniRowReader(in, columnCount);
    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
    braw.resetValid(columnCount);
    int indx = 0;
    while (reader.next(rowID)) {
      reader.getCurrentRow(braw);
      Object actualRow = serde.deserialize(braw);
      Object expectedRow = rows.get(indx);
      if (0 != ObjectInspectorUtils
          .compare(expectedRow, rowHiveObjectInspector, actualRow, out_oi,
              new FullMapEqualComparer())) {
        System.out.println("expected = "
            + SerDeUtils.getJSONString(expectedRow, rowHiveObjectInspector));
        System.out.println("actual = "
            + SerDeUtils.getJSONString(actualRow, out_oi));
        Assert.fail("Deserialized object does not compare");
      }
      indx++;
    }
    in.close();
    log.info("Done");
  }

  @Test
  public void testTrevni() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException {
    doTrevniFullReadTest(ColumnarSerDe.class, HadoopInput.class);
    doTrevniFullReadTest(LazyBinaryColumnarSerDe.class, HadoopInput.class);
    doTrevniFullReadTest(ColumnarSerDe.class, HadoopInput2.class);
    doTrevniFullReadTest(LazyBinaryColumnarSerDe.class, HadoopInput2.class);
  }

}
