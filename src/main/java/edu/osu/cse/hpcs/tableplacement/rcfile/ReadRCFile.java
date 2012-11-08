package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class ReadRCFile {
  private final Logger LOG = Logger.getLogger(ReadRCFile.class);

  private final Configuration conf = new Configuration();

  private ColumnarSerDeBase serDe;

  private Path file;

  private FileSystem fs;

  private Properties tbl;

  private int numCols;
  private int rowGroupSize;
  private int ioBufferSize;

  public ReadRCFile(String rcfileName, int numCols, int rowGroupSize,
      int ioBufferSize, String pathStr) {
    try {
      this.rowGroupSize = rowGroupSize;
      this.ioBufferSize = ioBufferSize;
      conf.setInt("hive.io.rcfile.record.buffer.size", this.rowGroupSize);
      conf.setInt("io.file.buffer.size", this.ioBufferSize);
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(pathStr);
      file = new Path(dir, rcfileName);
      // fs.delete(dir, true);
      // the SerDe part is from TestLazySimpleSerDe
      serDe = new ColumnarSerDe();
      // Create the SerDe
      this.numCols = numCols;
      tbl = createProperties();
      serDe.initialize(conf, tbl);

    } catch (Exception e) {

    }
  }

  private Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    String colStr = "c0";
    String colTypeStr = "string";
    LOG.info("numCols 3:" + numCols);
    for (int i = 1; i < numCols; i++) {
      colStr += ",c" + i;
      colTypeStr += ":string";
    }
    tbl.setProperty("columns", colStr);
    tbl.setProperty("columns.types", colTypeStr);
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    LOG.info("colStr:" + colStr);
    LOG.info("colTypeStr:" + colTypeStr);
    return tbl;
  }

  public void read(java.util.ArrayList<Integer> readCols) throws IOException,
      SerDeException {

    if (readCols.size() == 0) {
      ColumnProjectionUtils.setFullyReadColumns(conf);
    } else {
      ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    }
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    // int count = 0;
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(numCols); // Assume there are 1 fields
      Object row = serDe.deserialize(cols);
      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      // System.out.println("fieldRefs.size()" + fieldRefs.size());
      for (int i : readCols) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils
            .copyToStandardObject(fieldData, fieldRefs.get(i)
                .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        // System.out.println(standardWritableData.toString());
      }
      /*
       * if (count == 2){ System.exit(0); } count++;
       */
    }
    reader.close();
  }
}
