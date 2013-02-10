package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevni;

public class TrevniMultiFileWriter extends MultiFileWriter<ColumnFileWriter> {

  private static Logger log = Logger.getLogger(TrevniMultiFileWriter.class);

  Map<String, FSDataOutputStream> files;
  public TrevniMultiFileWriter(Configuration conf,
      Path outDir) throws IOException, InstantiationException, IllegalAccessException,
      SerDeException, ClassNotFoundException, TablePropertyException {
    super(conf, outDir);
    files = new LinkedHashMap<String, FSDataOutputStream>();
    for (int i=0; i<columnFileGroups.size(); i++) {      
      ColumnFileGroup group = columnFileGroups.get(i);
      Configuration groupConf = writeConf.get(group.getName());
      List<Column> columns = group.getColumns();
      ColumnMetaData[] metaData =
          WriteTrevni.createColumnMetaData(columns, columns.size());
      ColumnFileWriter writer = new ColumnFileWriter(
          WriteTrevni.createFileMeta("null", "null"),
          metaData);
      writers.put(group.getName(), writer);

      FSDataOutputStream file = fs.create(outputFiles[i], true,
          tableProp.getInt(
              TableProperty.HADOOP_IO_BUFFER_SIZE,
              TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE),   // IO buffer size, replication,
          (short) groupConf.getInt("dfs.replication", 1),          // number of replicas
          groupConf.getLong("dfs.block.size", 128 * 1024 * 1024),  // HDFS block size
          null);
      files.put(group.getName(), file);
    }
  }

  @Override
  public void append(Map<String, BytesRefArrayWritable> vals) throws IOException {
    for (Entry<String, BytesRefArrayWritable> entry: vals.entrySet()) {
      String groupName = entry.getKey();
      long[] groupSerializedSize = serializedSize[serializedSizeMapping.get(groupName)];
      ColumnFileWriter writer = writers.get(groupName);
      if (writer != null) {
        BytesRefArrayWritable val = entry.getValue();
        ByteBuffer[] buffer = new ByteBuffer[val.size()];
        for (int j = 0; j < val.size(); j++) {
          BytesRefWritable ref = val.get(j);
          buffer[j] = ByteBuffer.wrap(ref.getData(), ref.getStart(),
              ref.getLength());
          groupSerializedSize[j] += ref.getLength();
        }
        writer.writeRow((Object[]) buffer);
      } else {
        throw new IOException("Trevni column file writer for column file group " +
            groupName + " has not been defined");
      }
    }    
  }

  @Override
  public void close() throws IOException {
    for (Entry<String, ColumnFileWriter> entry: writers.entrySet()) {
      String groupName = entry.getKey();
      log.info("Write data of " + groupName + " from buffer to filesystem");
      ColumnFileWriter writer = entry.getValue();
      FSDataOutputStream file = files.get(groupName);
      writer.writeTo(file);  // Write data from in memory buffer to file
      file.close();          // close the output stream
    }
  }

}
