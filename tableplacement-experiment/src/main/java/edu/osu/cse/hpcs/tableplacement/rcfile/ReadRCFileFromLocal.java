package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ReadFromLocal;
import edu.osu.cse.hpcs.tableplacement.ReadFromLocalFactory;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class ReadRCFileFromLocal extends ReadFromLocal {
  protected static Logger log = Logger.getLogger(ReadRCFileFromLocal.class);

  public ReadRCFileFromLocal(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
  	super(propertyFilePath, inputPath, cmdProperties, log);
  }

  public long doRead() throws IOException, SerDeException {
	  long ts;
    assert totalRowReadTimeInNano == 0;
    assert totalRowDeserializationTimeInNano == 0;
	  
    RCFile.Reader reader = new RCFile.Reader(localFS, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable braw = new BytesRefArrayWritable(columnCount);
    braw.resetValid(columnCount);
    rowCount = 0;
    long totalSerializedDataSize = 0;

    while (reader.next(rowID)) {
      ts = System.nanoTime();
      reader.getCurrentRow(braw);
      totalRowReadTimeInNano += System.nanoTime() - ts;
      
      for (int j = 0; j < braw.size(); j++) {
        totalSerializedDataSize += braw.get(j).getLength();
      }
      
      ts = System.nanoTime();
      @SuppressWarnings("unused")
      Object row = serde.deserialize(braw);
      totalRowDeserializationTimeInNano += System.nanoTime() - ts;
      
      rowCount++;
    }
    reader.close();
    log.info("Row count : " + rowCount);
    log.info("Total serialized data size: " + totalSerializedDataSize);
    return totalSerializedDataSize;
  }

  public String getFormatName() {
    return "RCFile";
  }
  
  public static void main(String[] args) throws Exception {
    ReadRCFileFromLocal readRCFileLocal = (ReadRCFileFromLocal) ReadFromLocalFactory.get(args, ReadRCFileFromLocal.class);
    readRCFileLocal.runTest();
  }
}
