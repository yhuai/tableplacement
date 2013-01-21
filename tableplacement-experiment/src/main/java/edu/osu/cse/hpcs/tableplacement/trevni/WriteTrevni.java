package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;

import edu.osu.cse.hpcs.tableplacement.WriteTo;
import edu.osu.cse.hpcs.tableplacement.WriteFactory;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.multifile.TrevniMultiFileWriter;

/**
 * Use Trevni with Hive SerDe (Columns defined in trevni are
 * org.apache.trevni.ValueType.BYTES)
 * 
 */
public class WriteTrevni extends WriteTo {

  public static ColumnFileMetaData createFileMeta(String codec, String checksum) {
    return new ColumnFileMetaData().setCodec(codec).setChecksum(checksum);
  }

  public static ColumnMetaData[] createColumnMetaData(List<Column> columns,
      int columnCount) {
    ColumnMetaData[] columnMetadata = new ColumnMetaData[columnCount];
    for (int i = 0; i < columnCount; i++) {
      Column col = columns.get(i);
      columnMetadata[i] = new ColumnMetaData(col.getName(), ValueType.BYTES);
    }
    return columnMetadata;
  }

  protected static Logger log = Logger.getLogger(WriteTrevni.class);

  public WriteTrevni(String propertyFilePath, String outputPath,
      long rowCount, Properties cmdProperties) throws IOException,
      TablePropertyException, SerDeException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    super(propertyFilePath, outputPath, rowCount, cmdProperties, log);
  }

  public long write() throws IOException, SerDeException, InstantiationException, IllegalAccessException, ClassNotFoundException, TablePropertyException {
    TrevniMultiFileWriter writer = new TrevniMultiFileWriter(conf, outputDir);
    return doWrite(writer, log);
  }

  public String getFormatName() {
    return "Trevni";
  }

  public static void main(String[] args) throws Exception {
    WriteTrevni writeTrevni = (WriteTrevni) WriteFactory
        .get(args, WriteTrevni.class);
    writeTrevni.runTest();
  }
}
