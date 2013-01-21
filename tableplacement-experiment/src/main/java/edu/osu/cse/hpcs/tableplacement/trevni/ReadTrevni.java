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
import edu.osu.cse.hpcs.tableplacement.multifile.RCFileMultiFileReader;
import edu.osu.cse.hpcs.tableplacement.multifile.TrevniMultiFileReader;

public class ReadTrevni extends ReadFrom {
  protected static Logger log = Logger.getLogger(ReadTrevni.class);

  public ReadTrevni(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    super(propertyFilePath, inputPath, cmdProperties, log);
  }

  public long read() throws IOException, SerDeException, ClassNotFoundException, InstantiationException, IllegalAccessException, TablePropertyException {
    // For localFS, it uses 
    // org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker.ChecksumFSInputChecker.
    // But it will open and close the file for every read operation.
    // We may need to just use File instead of HadoopInput for local test.
    TrevniMultiFileReader reader = new TrevniMultiFileReader(conf, inputDir, readColumns);
    return doRead(reader, log);
  }
  
  public String getFormatName() {
    return "Trevni";
  }

  public static void main(String[] args) throws Exception {
    ReadTrevni readTrevniLocal = (ReadTrevni) ReadFactory.get(args, ReadTrevni.class);
    readTrevniLocal.runTest();
  }
}
