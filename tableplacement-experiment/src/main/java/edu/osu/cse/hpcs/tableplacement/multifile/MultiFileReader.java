package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;

public abstract class MultiFileReader<T> {

  protected TableProperty tableProp;
  protected FileSystem fs;
  protected Configuration conf;
  protected Path inputDir;
  protected Path[] inputFiles;
  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<String, T> readers;
  protected Map<String, ColumnFileGroup> columnFileGroupsMap;
  protected Map<String, Path> columnFileGroupFiles;
  protected Map<String, List<Integer>> readColumns;
  protected Map<String, Configuration> readConf;

  public MultiFileReader(TableProperty prop, Configuration conf, Path inDir,
      Map<String, List<Integer>> readColumns)
        throws IOException {
    tableProp = prop;
    columnFileGroups = tableProp.getColumnFileGroups();
    columnFileGroupsMap = new LinkedHashMap<String, ColumnFileGroup>();
    columnFileGroupFiles = new LinkedHashMap<String, Path>();
    readConf = new LinkedHashMap<String, Configuration>(); 
    inputDir = inDir;
    fs = inputDir.getFileSystem(conf);
    inputFiles = new Path[columnFileGroups.size()];
    readers = new LinkedHashMap<String, T>();
    for (int i=0; i<columnFileGroups.size(); i++) {
      ColumnFileGroup group = columnFileGroups.get(i);
      columnFileGroupsMap.put(group.getName(), group);
      inputFiles[i] = new Path(inputDir, group.getName());
      columnFileGroupFiles.put(group.getName(), inputFiles[i]);
    }
    this.readColumns = readColumns;
    for (Entry<String, List<Integer>> entry: readColumns.entrySet()) {
      String groupName = entry.getKey();
      Configuration thisConf = new Configuration(conf);
      ColumnProjectionUtils.setReadColumnIDs(thisConf, new ArrayList(entry.getValue()));
      readConf.put(groupName, thisConf); 
    }
  }
  
  /**
   * Check if there is a row for read
   * @param rowID the object used to store next row's id
   * @return true if there is a row for read
   */
  public abstract boolean next(LongWritable rowID);
  
  /**
   * Check if there is a value for read for a specific column in a
   * specific column file group 
   * @param rowID rowID the object used to store next row's id
   * @param groupName the name of the column file group
   * @param column the id of the column in the column file group
   * @return true if there is a value for read
   */
  public abstract boolean next(LongWritable rowID, String groupName, int column);
  
  /**
   * Read a row. Make sure call {@link #next(LongWritable)} first.
   * @param ret
   */
  public abstract void getCurrentRow(BytesRefArrayWritable ret);
  
  /**
   * Read a row. Make sure call {@link #next(LongWritable)} first.
   * @param ret
   */
  public abstract void getValue(BytesRefArrayWritable ret);
  
}
