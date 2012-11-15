package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.WriteToLocal;
import edu.osu.cse.hpcs.tableplacement.WriteToLocalFactory;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class WriteRCFileToLocal extends WriteToLocal {

  protected static Logger log = Logger.getLogger(WriteRCFileToLocal.class);

  public WriteRCFileToLocal(String propertyFilePath, String outputPath,
      long rowCount, Properties cmdProperties) throws IOException,
      TablePropertyException, SerDeException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    super(propertyFilePath, outputPath, rowCount, cmdProperties, log);
    RCFileOutputFormat.setColumnNumber(conf, columnCount);
  }

  public long doWrite() throws IOException, SerDeException {
    long totalSerializedDataSize = 0;
    long[] columnSerializedDataSize = new long[columnCount];
    long ts;
    assert totalRowGenerationTimeInNano == 0;
    assert totalRowSerializationTimeInNano == 0;
    
    RCFile.Writer writer = new RCFile.Writer(localFS, conf, file, null, null);
    for (long i = 0; i < rowCount; i++) {
      ts = System.nanoTime();
      List<Object> row = new ArrayList<Object>(columnCount);
      for (Column col : columns) {
        row.add(col.nextValue());
      }
      totalRowGenerationTimeInNano += System.nanoTime() - ts;
      
      ts = System.nanoTime();
      BytesRefArrayWritable bytes = (BytesRefArrayWritable) serde.serialize(
          row, rowHiveObjectInspector);
      totalRowSerializationTimeInNano += System.nanoTime() - ts;
      
      for (int j = 0; j < bytes.size(); j++) {
        BytesRefWritable ref = bytes.get(j);
        int length = ref.getLength();
        totalSerializedDataSize += length;
        columnSerializedDataSize[j] += length;
      }
      writer.append(bytes);
    }

    writer.close();
    log.info("Total serialized data size: " + totalSerializedDataSize);
    for (int i = 0; i < columnCount; i++) {
      log.info("Column " + i + " serialized data size: "
          + columnSerializedDataSize[i]);
    }
    return totalSerializedDataSize;
  }

  public String getFormatName() {
    return "RCFile";
  }

  public static void main(String[] args) throws Exception {
    WriteRCFileToLocal writeRCFileLocal = (WriteRCFileToLocal) WriteToLocalFactory
        .get(args, WriteRCFileToLocal.class);
    writeRCFileLocal.runTest();
  }
}
