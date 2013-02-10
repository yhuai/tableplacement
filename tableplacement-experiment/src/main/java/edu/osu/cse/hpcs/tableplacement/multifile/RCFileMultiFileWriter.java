package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

/*
 * A write which can write a table to multiple files in RCFile format.
 */
public class RCFileMultiFileWriter extends MultiFileWriter<RCFile.Writer> {

  private static Logger log = Logger.getLogger(RCFileMultiFileWriter.class);
  
  public RCFileMultiFileWriter(Configuration conf, Path outputDir)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException,
      ClassNotFoundException, TablePropertyException {
    super(conf, outputDir);
    for (int i=0; i<columnFileGroups.size(); i++) {
      //writers[i] = new RCFile.Writer(fs, conf, outputFiles[i], null, null);
      ColumnFileGroup group = columnFileGroups.get(i);
      
      Configuration groupConf = writeConf.get(group.getName());
      RCFile.Writer writer = new RCFile.Writer(fs, groupConf, outputFiles[i], 
          tableProp.getInt(
              TableProperty.HADOOP_IO_BUFFER_SIZE,
              TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE),   // IO buffer size
          (short) groupConf.getInt("dfs.replication", 1),          // number of replicas
          groupConf.getLong("dfs.block.size", 128 * 1024 * 1024),  // HDFS block size
          null, new Metadata(), null);
      writers.put(group.getName(), writer);
    }
  }

  @Override
  public void append(Map<String, BytesRefArrayWritable> vals) throws IOException {
    for (Entry<String, BytesRefArrayWritable> entry: vals.entrySet()) {
      String groupName = entry.getKey();
      long[] groupSerializedSize = serializedSize[serializedSizeMapping.get(groupName)];
      RCFile.Writer writer = writers.get(groupName);
      if (writer != null) {
        BytesRefArrayWritable val = entry.getValue();
        for (int j = 0; j < val.size(); j++) {
          BytesRefWritable ref = val.get(j);
          groupSerializedSize[j] += ref.getLength();
        }
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
