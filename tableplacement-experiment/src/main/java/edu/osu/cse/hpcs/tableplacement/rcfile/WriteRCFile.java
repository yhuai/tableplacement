package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.WriteTo;
import edu.osu.cse.hpcs.tableplacement.WriteFactory;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.RCFileMultiFileWriter;

public class WriteRCFile extends WriteTo {

  protected static Logger log = Logger.getLogger(WriteRCFile.class);

  public WriteRCFile(String propertyFilePath, String outputPath,
      long rowCount, Properties cmdProperties) throws IOException,
      TablePropertyException, SerDeException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    super(propertyFilePath, outputPath, rowCount, cmdProperties, log);
    RCFileOutputFormat.setColumnNumber(conf, columnCount);
  }

  public long write() throws IOException, SerDeException, InstantiationException, IllegalAccessException, ClassNotFoundException, TablePropertyException {
    RCFileMultiFileWriter writer = new RCFileMultiFileWriter(conf, outputDir);
    return doWrite(writer, log);
  }

  public String getFormatName() {
    return "RCFile";
  }

  public static void main(String[] args) throws Exception {
    WriteRCFile writeRCFile = (WriteRCFile) WriteFactory
        .get(args, WriteRCFile.class);
    writeRCFile.runTest();
  }
}
