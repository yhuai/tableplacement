package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.avro.HadoopInput;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.ReadFrom;
import edu.osu.cse.hpcs.tableplacement.ReadFactory;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.MultiFileReader;
import edu.osu.cse.hpcs.tableplacement.multifile.RCFileMultiFileReader;
import edu.osu.cse.hpcs.tableplacement.multifile.TrevniMultiFileColumnReader;
import edu.osu.cse.hpcs.tableplacement.multifile.TrevniMultiFileRowReader;

public class ReadTrevniColumnOriented extends ReadFrom {
  protected static Logger log = Logger.getLogger(ReadTrevniColumnOriented.class);

  public ReadTrevniColumnOriented(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    super(propertyFilePath, inputPath, cmdProperties, log);
  }
  
  @Override
  public long doRead(MultiFileReader reader, Logger log)
      throws IOException, SerDeException {
    long ts;
    totalRowReadTimeInNano = 0;
    totalInitializationTimeInNano = 0;
    totalCalculateSizeTimeInNano = 0;
    totalDataReadTimeInNano = 0;
    //assert totalRowDeserializationTimeInNano == 0;

    ts = System.nanoTime();
    Map<String, BytesRefArrayWritable> ret = new HashMap<String, BytesRefArrayWritable>();
    List<ColumnFileGroup> groups = reader.getColumnFileGroups();
    Map<String, List<Integer>> readColumns = reader.getReadColumns();
    for (ColumnFileGroup group: groups) {
      if (!readColumns.keySet().contains(group.getName())) {
        continue;
      }
      BytesRefArrayWritable braw = new BytesRefArrayWritable(group.getColumns().size());
      braw.resetValid(group.getColumns().size());
      ret.put(group.getName(), braw);
    }
    rowCount = 0;
    long totalSerializedDataSize = 0;
    totalInitializationTimeInNano = (System.nanoTime() - ts);
    
    long start = System.nanoTime();
    for (Entry<String, List<Integer>> groupCols: readColumns.entrySet()) {
      String groupName = groupCols.getKey();
      for (Integer col: groupCols.getValue()) {
        LongWritable rowID = new LongWritable();
        long columnSerializedDataSize = 0;
        long columnReadTimeInNano = 0;
        long startReadColumn = 0;
        while (reader.next(rowID, groupName, col)) {
          ts = System.nanoTime();
          reader.getCurrentColumnValue(ret, groupName, col);
          totalRowReadTimeInNano += System.nanoTime() - ts;
          ts = System.nanoTime();
          long size = ret.get(groupName).get(col).getLength();
          totalSerializedDataSize += size;
          columnSerializedDataSize += size;
          totalCalculateSizeTimeInNano += System.nanoTime() - ts;
        }
        columnReadTimeInNano = System.nanoTime() - startReadColumn;
        log.info("Elapsed time on reading column " + col + 
            " in file group " + groupName + ": " + columnReadTimeInNano);
        log.info("Serialized data size of column " + col + 
            " in file group " + groupName + ": " + columnSerializedDataSize);
      }
    }
    totalDataReadTimeInNano = System.nanoTime() - start;
    ts = System.nanoTime();
    reader.close();
    readerCloseTimeInNano = System.nanoTime() - ts;
    log.info("Total serialized data size: " + totalSerializedDataSize);
    return totalSerializedDataSize;
  }

  public long read() throws IOException, SerDeException, ClassNotFoundException, InstantiationException, IllegalAccessException, TablePropertyException {
    // For localFS, it uses 
    // org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker.ChecksumFSInputChecker.
    // But it will open and close the file for every read operation.
    // We may need to just use File instead of HadoopInput for local test.
    long ts = System.nanoTime();
    TrevniMultiFileColumnReader reader =
        new TrevniMultiFileColumnReader(conf, inputDir, readColumnsStr, isReadLocalFS);
    readerCreateTimeInNano = System.nanoTime() - ts;
    return doRead(reader, log);
  }
  
  public String getFormatName() {
    return "Trevni";
  }

  public static void main(String[] args) throws Exception {
    ReadTrevniColumnOriented readTrevni = (ReadTrevniColumnOriented) ReadFactory.get(args, ReadTrevniColumnOriented.class);
    readTrevni.runTest();
  }
}
