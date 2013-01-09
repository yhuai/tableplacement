package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.avro.HadoopInput;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.trevni.TrevniRowReader;

public class TrevniMultiFileReader extends MultiFileReader<ColumnFileReader> {

  private Map<String, TrevniRowReader> rowReaders;

  public TrevniMultiFileReader(Configuration conf, Path inDir,
      Map<String, List<Integer>> readColumns) throws IOException,
      ClassNotFoundException, SerDeException, InstantiationException,
      IllegalAccessException, TablePropertyException {
    super(conf, inDir, readColumns);
    rowReaders = new LinkedHashMap<String, TrevniRowReader>();
    for (String groupName: readColumns.keySet()) {
      ColumnFileGroup group = columnFileGroupsMap.get(groupName);
      Path file = columnFileGroupFiles.get(groupName);
      Configuration groupConf = readConf.get(groupName);
      groupConf.setInt(
              TableProperty.HADOOP_IO_BUFFER_SIZE,
              TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE);   // IO buffer size
      ColumnFileReader reader = new ColumnFileReader(new HadoopInput(file, groupConf));
      readers.put(groupName, reader);
      List<Integer> readCols = readColumns.get(groupName);
      rowReaders.put(groupName,
          new TrevniRowReader(reader, group.getColumns().size(), readCols));
    }
  }

  @Override
  public boolean next(LongWritable rowID) throws IOException {
    boolean ret = true;
    for (Entry<String, TrevniRowReader> entry: rowReaders.entrySet()) {
      TrevniRowReader reader = entry.getValue();
      ret = reader.next(rowID) && ret;
    }
    return ret;
  }

  @Override
  public boolean next(LongWritable rowID, String groupName, int column) {
    // TODO to be implemented
    return false;
  }

  @Override
  public void getCurrentRow(Map<String, BytesRefArrayWritable> ret)
      throws IOException {
    for (Entry<String, TrevniRowReader> entry: rowReaders.entrySet()) {
      String groupName = entry.getKey();
      TrevniRowReader reader = entry.getValue();
      BytesRefArrayWritable braw = ret.get(groupName);
      reader.getCurrentRow(braw);
    }
  }

  @Override
  public void getCurrentColumnValue(BytesRefArrayWritable ret,
      String groupName, int column) {
    // TODO to be implemented
    
  }

  @Override
  public void close() throws IOException {
    for (ColumnFileReader reader: readers.values()) {
      reader.close();
    }
  }

}
