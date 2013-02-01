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
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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

import edu.osu.cse.hpcs.tableplacement.BaseFormatTestClass;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

@RunWith(value = Parameterized.class)
public class TestTrevni extends BaseFormatTestClass {

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

  private void writeTrevniFile(
      ColumnarSerDeBase serde,
      StandardStructObjectInspector rowHiveObjectInspector,
      List<List<Object>> rows) throws IOException, SerDeException {

    log.info("Writing Trevni ...");
    int totalSerializedDataSize = 0;
    ColumnFileWriter out = new ColumnFileWriter(
        WriteTrevni.createFileMeta(codec, checksum),
        WriteTrevni.createColumnMetaData(columns, columnCount));
    FSDataOutputStream trevniOutputStream = localFS.create(path);

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
  }
  
  private void doTrevniFullReadTest(Class<?> serDeClass, Class<?> inputClass)
      throws InstantiationException, IllegalAccessException, SerDeException,
      IOException {

    log.info("Testing Trevni write and full read with ColumnarSerDe class "
        + serDeClass.getCanonicalName() + " and InputClass " + inputClass.getCanonicalName());
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());

    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = getTest4ColRows(rowCount, 3);
    writeTrevniFile(serde, rowHiveObjectInspector, rows);

    log.info("Reading Trevni ...");
    ObjectInspector out_oi = serde.getObjectInspector();
    log.info("FileSystem: " + path.getFileSystem(hadoopConf).getClass());
    assert path.getFileSystem(hadoopConf) instanceof LocalFileSystem;

    ColumnFileReader in;
    if (HadoopInput2.class.equals(inputClass)) {
      in = new ColumnFileReader(
          new HadoopInput2(path, hadoopConf));
    } else {
      in = new ColumnFileReader(
          new HadoopInput(path, hadoopConf));
    }
    
    ColumnMetaData[] metadata = in.getColumnMetaData();
    for (int i = 0; i < metadata.length; i++) {
      log.info(metadata[i].getName() + " " + metadata[i].getType() + " "
          + metadata[i].getNumber());
    }
    Assert.assertEquals(rowCount, in.getRowCount());
    Assert.assertEquals(columnCount, in.getColumnCount());

    ColumnProjectionUtils.setFullyReadColumns(hadoopConf);
    // initialize again since notSkipIDs has been changed and need to be retrieved again.
    serde.initialize(hadoopConf, testTableProperty.getProperties());

    TrevniRowReader reader = new TrevniRowReader(in, columnCount);
    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
    braw.resetValid(columnCount);
    int indx = 0;
    while (reader.next(rowID)) {
      reader.getCurrentRow(braw, 0);
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
  
  private void doTrevniPartialReadTest(Class<?> serDeClass, Class<?> inputClass)
      throws InstantiationException, IllegalAccessException, SerDeException,
      IOException {

    log.info("Testing Trevni write and partial read with ColumnarSerDe class "
        + serDeClass.getCanonicalName() + " and InputClass " + inputClass.getCanonicalName());
    serde = (ColumnarSerDeBase) serDeClass.newInstance();
    serde.initialize(hadoopConf, testTableProperty.getProperties());
    StandardStructObjectInspector rowHiveObjectInspector = (StandardStructObjectInspector) testTableProperty
        .getHiveRowObjectInspector();

    List<List<Object>> rows = getTest4ColRows(rowCount, 3);
    writeTrevniFile(serde, rowHiveObjectInspector, rows);

    log.info("Reading Trevni ...");
    ObjectInspector out_oi = serde.getObjectInspector();
    log.info("FileSystem: " + path.getFileSystem(hadoopConf).getClass());
    assert path.getFileSystem(hadoopConf) instanceof LocalFileSystem;

    ColumnFileReader in;
    if (HadoopInput2.class.equals(inputClass)) {
      in = new ColumnFileReader(
          new HadoopInput2(path, hadoopConf));
    } else {
      in = new ColumnFileReader(
          new HadoopInput(path, hadoopConf));
    }
    
    ColumnMetaData[] metadata = in.getColumnMetaData();
    for (int i = 0; i < metadata.length; i++) {
      log.info(metadata[i].getName() + " " + metadata[i].getType() + " "
          + metadata[i].getNumber());
    }
    Assert.assertEquals(rowCount, in.getRowCount());
    Assert.assertEquals(columnCount, in.getColumnCount());

    for (int i=0; i<columnCount; i++) {
      log.info("Read column " + i + " with row reader");
      ColumnProjectionUtils.setReadColumnIDs(hadoopConf, new ArrayList(Arrays.asList(i)));
      // initialize again since notSkipIDs has been changed and need to be retrieved again.
      serde.initialize(hadoopConf, testTableProperty.getProperties());
      TrevniRowReader reader = new TrevniRowReader(in, columnCount, Arrays.asList(i));
      LongWritable rowID = new LongWritable();
      BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
      braw.resetValid(columnCount);
      int indx = 0;
      while (reader.next(rowID)) {
        reader.getCurrentRow(braw, 0);
        Object actualRow = serde.deserialize(braw);
        StructObjectInspector oi = (StructObjectInspector) out_oi;
        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
        Object fieldData = oi.getStructFieldData(actualRow, fieldRefs.get(i));
        Object javaObjectData = 
            ObjectInspectorUtils.copyToStandardJavaObject(fieldData,
                fieldRefs.get(i).getFieldObjectInspector());

        List<Object> expectedRow = rows.get(indx);
        Assert.assertEquals(expectedRow.get(i), javaObjectData);
        indx++;
      }
      in.close();
    }

    for (int i=0; i<columnCount; i++) {
      log.info("Read column " + i + " with column reader");
      ColumnProjectionUtils.setReadColumnIDs(hadoopConf, new ArrayList(Arrays.asList(i)));
      // initialize again since notSkipIDs has been changed and need to be retrieved again.
      serde.initialize(hadoopConf, testTableProperty.getProperties());
      TrevniColumnReader reader = new TrevniColumnReader(in, columnCount, Arrays.asList(i));
      LongWritable rowID = new LongWritable();
      BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
      braw.resetValid(columnCount);
      int indx = 0;
      while (reader.next(rowID, i)) {
        reader.getCurrentColumnValue(braw, i);
        Object actualRow = serde.deserialize(braw);
        StructObjectInspector oi = (StructObjectInspector) out_oi;
        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
        Object fieldData = oi.getStructFieldData(actualRow, fieldRefs.get(i));
        Object javaObjectData = 
            ObjectInspectorUtils.copyToStandardJavaObject(fieldData,
                fieldRefs.get(i).getFieldObjectInspector());

        List<Object> expectedRow = rows.get(indx);
        Assert.assertEquals(expectedRow.get(i), javaObjectData);
        indx++;
      }
      in.close();
    }

    log.info("Done");
  }

  @Test
  public void testTrevni() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException, InterruptedException {
    doTrevniFullReadTest(ColumnarSerDe.class, HadoopInput.class);
    doTrevniFullReadTest(LazyBinaryColumnarSerDe.class, HadoopInput.class);
    doTrevniPartialReadTest(ColumnarSerDe.class, HadoopInput.class);
    doTrevniPartialReadTest(LazyBinaryColumnarSerDe.class, HadoopInput.class);

    doTrevniFullReadTest(ColumnarSerDe.class, HadoopInput2.class);
    doTrevniFullReadTest(LazyBinaryColumnarSerDe.class, HadoopInput2.class);
  }

}
