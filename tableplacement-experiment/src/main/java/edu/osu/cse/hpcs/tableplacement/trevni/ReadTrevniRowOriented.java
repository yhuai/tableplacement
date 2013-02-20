package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.avro.HadoopInput;

import edu.osu.cse.hpcs.tableplacement.ReadFrom;
import edu.osu.cse.hpcs.tableplacement.ReadFactory;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.TrevniMultiFileRowReader;


public class ReadTrevniRowOriented extends ReadFrom {
  protected static Logger log = Logger.getLogger(ReadTrevniRowOriented.class);

  public ReadTrevniRowOriented(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    super(propertyFilePath, inputPath, cmdProperties, log);
  }

  public long read() throws IOException, SerDeException, ClassNotFoundException, InstantiationException, IllegalAccessException, TablePropertyException, InterruptedException {
    // For localFS, it uses 
    // org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker.ChecksumFSInputChecker.
    // But it will open and close the file for every read operation.
    // We may need to just use File instead of HadoopInput for local test.
    long ts = System.nanoTime();
    TrevniMultiFileRowReader reader = new TrevniMultiFileRowReader(conf, inputDir, readColumnsStr, isReadLocalFS);
    readerCreateTimeInNano = System.nanoTime() - ts;
    return doRead(reader, log);
  }
  
  public String getFormatName() {
    return "Trevni";
  }

  public static void main(String[] args) throws Exception {
    ReadTrevniRowOriented readTrevni = (ReadTrevniRowOriented) ReadFactory.get(args, ReadTrevniRowOriented.class);
    readTrevni.runTest();
  }
}
