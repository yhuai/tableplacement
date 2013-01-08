package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

/*
 * A write which can write a table to multiple files in RCFile format.
 */
public class RCFileMultiFileWriter extends MultiFileWriter {

  private RCFile.Writer[] writers;
  public RCFileMultiFileWriter(TableProperty prop, Configuration conf, Path outputDir)
      throws IOException {
    super(prop, conf, outputDir);
    writers = new RCFile.Writer[outputFiles.length];
    for (int i=0; i<writers.length; i++) {
      //writers[i] = new RCFile.Writer(fs, conf, outputFiles[i], null, null);
      writers[i] = new RCFile.Writer(fs, conf, outputFiles[i], 
          prop.getInt(
              TableProperty.HADOOP_IO_BUFFER_SIZE,
              TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE),   // IO buffer size
          (short) conf.getInt("dfs.replication", 3),          // number of replicas
          conf.getLong("dfs.block.size", 128 * 1024 * 1024),  // HDFS block size
          null, new Metadata(), null);
    }
  }
}
