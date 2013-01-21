package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.ReadFrom;
import edu.osu.cse.hpcs.tableplacement.ReadFactory;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.MultiFileReader;
import edu.osu.cse.hpcs.tableplacement.multifile.RCFileMultiFileReader;

public class ReadRCFile extends ReadFrom {
  protected static Logger log = Logger.getLogger(ReadRCFile.class);

  public ReadRCFile(String propertyFilePath, String inputPath,
      Properties cmdProperties) throws IOException, TablePropertyException,
      SerDeException, InstantiationException, IllegalAccessException,
      ClassNotFoundException {
  	super(propertyFilePath, inputPath, cmdProperties, log);
  }

  public long read() throws IOException, SerDeException, ClassNotFoundException, InstantiationException, IllegalAccessException, TablePropertyException {
    RCFileMultiFileReader reader = new RCFileMultiFileReader(conf, inputDir, readColumns);
    return doRead(reader, log);
  }

  public String getFormatName() {
    return "RCFile";
  }
  
  public static void main(String[] args) throws Exception {
    ReadRCFile readRCFile = (ReadRCFile) ReadFactory.get(args, ReadRCFile.class);
    readRCFile.runTest();
  }
}
