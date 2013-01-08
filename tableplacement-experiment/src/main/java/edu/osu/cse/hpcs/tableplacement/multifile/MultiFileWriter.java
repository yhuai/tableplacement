package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;

public abstract class MultiFileWriter<T> {

  protected TableProperty tableProp;
  protected FileSystem fs;
  protected Configuration conf;
  protected Path outputDir;
  protected Path[] outputFiles;
  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<String, T> writers;

  public MultiFileWriter(TableProperty prop, Configuration conf, Path outDIr)
      throws IOException {
    tableProp = prop;
    columnFileGroups = tableProp.getColumnFileGroups();
    outputDir = outDIr;
    fs = outputDir.getFileSystem(conf);
    outputFiles = new Path[columnFileGroups.size()];
    writers = new LinkedHashMap<String, T>();
    for (int i=0; i<columnFileGroups.size(); i++) {
      ColumnFileGroup group = columnFileGroups.get(i);
      outputFiles[i] = new Path(outputDir, group.getName());
    }
  }
  
  
  /**
   * Users need to serialize the row according to column file groups they defined first,
   * and then call append.
   * @param vals values to append. FOr every entry,
   * the key is the name of a column file group,
   * and the value is serialized values of the column file group.
   * @throws IOException 
   */
  public abstract void append(Map<String, BytesRefArrayWritable> vals) throws IOException;
  
  public abstract void close() throws IOException;
}
