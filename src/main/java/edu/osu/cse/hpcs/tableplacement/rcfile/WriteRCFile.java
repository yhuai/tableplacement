package edu.osu.cse.hpcs.tableplacement.rcfile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.log4j.Logger;

public class WriteRCFile {
  private final Logger LOG = Logger.getLogger(WriteRCFile.class);

  private final Configuration conf = new Configuration();

  private ColumnarSerDeBase serDe;

  private Path file;

  private FileSystem fs;

  private Properties tbl;

  private String rawFileName;
  private int numCol;
  private int rowGroupSize;
  private int ioBufferSize;

  public WriteRCFile(String rawFileName, String rcfileName, int numCol,
      int rowGroupSize, int ioBufferSize, String pathStr) {
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
      tbl = createProperties();
      serDe.initialize(conf, tbl);
      this.rawFileName = rawFileName;
      this.numCol = numCol;

    } catch (Exception e) {
    }
  }

  private static Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

  public void write() throws IOException, SerDeException {
    // fs.delete(file, true);

    byte[][] fieldsData = new byte[numCol][];
    RCFileOutputFormat.setColumnNumber(conf, numCol);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
        new DefaultCodec());

    try {
      // Create file
      FileReader fstream = new FileReader(rawFileName);
      BufferedReader in = new BufferedReader(fstream);
      String line = null; // not declared within while loop
      // int count = 0;
      while ((line = in.readLine()) != null) {
        // System.out.println("-----------");
        String[] cols = line.split("~");
        // System.out.println(cols.length);
        for (int i = 0; i < numCol; i++) {
          fieldsData[i] = cols[i].getBytes("UTF-8");
          // System.out.println(new String(fieldsData[i], "UTF-8") + "~~~" +
          // cols[i]);
        }
        BytesRefArrayWritable bytes = new BytesRefArrayWritable(
            fieldsData.length);
        for (int i = 0; i < fieldsData.length; i++) {
          BytesRefWritable cu = null;
          cu = new BytesRefWritable(fieldsData[i], 0, fieldsData[i].length);
          bytes.set(i, cu);
        }
        writer.append(bytes);

        /*
         * if (count == 2){ System.exit(-1); } count++;
         */
      }
      in.close();
    } catch (Exception e) {// Catch exception if any
      System.err.println("Error: " + e.getMessage());
    }
    writer.close();
  }

  public static void main(String[] args) throws Exception {
    WriteRCFile wRCFile = new WriteRCFile("narrow.txt",
        "4M-Row-group.narrow.rcfile", 10, 4 * 1024 * 1024, 128 * 1024,
        "/home/yhuai/Projects/HadoopLearning/workspace/RCFile-playground/Tables");
    wRCFile.write();

    wRCFile = new WriteRCFile("narrow-1stCol.txt",
        "4M-Row-group.narrow-1stCol.rcfile", 1, 4 * 1024 * 1024, 128 * 1024,
        "/home/yhuai/Projects/HadoopLearning/workspace/RCFile-playground/Tables");
    wRCFile.write();

    wRCFile = new WriteRCFile("wide.txt", "4M-Row-group.wide.rcfile", 100,
        4 * 1024 * 1024, 128 * 1024,
        "/home/yhuai/Projects/HadoopLearning/workspace/RCFile-playground/Tables");
    wRCFile.write();

    wRCFile = new WriteRCFile("wide-1stCol.txt",
        "4M-Row-group.wide-1stCol.rcfile", 1, 4 * 1024 * 1024, 128 * 1024,
        "/home/yhuai/Projects/HadoopLearning/workspace/RCFile-playground/Tables");
    wRCFile.write();
  }
}
