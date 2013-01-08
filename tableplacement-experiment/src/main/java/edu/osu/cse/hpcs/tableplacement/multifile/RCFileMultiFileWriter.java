package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;

/*
 * A write which can write a table to multiple files in RCFile format.
 */
public class RCFileMultiFileWriter extends MultiFileWriter<RCFile.Writer> {

  public RCFileMultiFileWriter(TableProperty prop, Configuration conf, Path outputDir)
      throws IOException {
    super(prop, conf, outputDir);
    for (int i=0; i<columnFileGroups.size(); i++) {
      //writers[i] = new RCFile.Writer(fs, conf, outputFiles[i], null, null);
      RCFile.Writer writer = new RCFile.Writer(fs, conf, outputFiles[i], 
          prop.getInt(
              TableProperty.HADOOP_IO_BUFFER_SIZE,
              TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE),   // IO buffer size
          (short) conf.getInt("dfs.replication", 3),          // number of replicas
          conf.getLong("dfs.block.size", 128 * 1024 * 1024),  // HDFS block size
          null, new Metadata(), null);
      ColumnFileGroup group = columnFileGroups.get(i);
      writers.put(group.getName(), writer);
    }
  }

  @Override
  public void append(Map<String, BytesRefArrayWritable> vals) throws IOException {
    for (Entry<String, BytesRefArrayWritable> entry: vals.entrySet()) {
      String groupName = entry.getKey();
      RCFile.Writer writer = writers.get(groupName);
      if (writer != null) {
        Writable val = entry.getValue();
        writer.append(val);
      } else {
        throw new IOException("RCFile writer for column file group " +
            groupName + " has not been defined");
      }
    }
  }

  @Override
  public void close() throws IOException {
    for (RCFile.Writer writer: writers.values()) {
      writer.close();
    }
  }
}
