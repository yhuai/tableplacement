package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.io.Writable;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public abstract class MultiFileWriter<T> {

  protected TableProperty tableProp;
  protected FileSystem fs;
  protected Configuration conf;
  protected Path outputDir;
  protected Path[] outputFiles;
  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<String, T> writers;
  protected Map<String, Configuration> writeConf;
  protected Map<String, ColumnarSerDeBase> serdes;

  public MultiFileWriter(Configuration conf, Path outDir)
      throws IOException, InstantiationException, IllegalAccessException, SerDeException,
      ClassNotFoundException, TablePropertyException {
    tableProp = new TableProperty(conf);
    tableProp.prepareColumns();
    String serDeClassName = tableProp.get(TableProperty.SERDE_CLASS);
    if (serDeClassName == null) {
      serDeClassName = TableProperty.DEFAULT_SERDE_CLASS;
    }
    Class serDeClass = Class.forName(serDeClassName);
    columnFileGroups = tableProp.getColumnFileGroups();
    outputDir = outDir;
    fs = outputDir.getFileSystem(conf);
    outputFiles = new Path[columnFileGroups.size()];
    writers = new LinkedHashMap<String, T>();
    serdes = new LinkedHashMap<String, ColumnarSerDeBase>();
    writeConf = new LinkedHashMap<String, Configuration>(); 
    
    for (int i=0; i<columnFileGroups.size(); i++) {
      ColumnFileGroup group = columnFileGroups.get(i);
      outputFiles[i] = new Path(outputDir, group.getName());
      ColumnarSerDeBase serde = (ColumnarSerDeBase) serDeClass.newInstance();
      serde.initialize(conf, group.getGroupProp().getProperties());
      serdes.put(group.getName(), serde);
      Configuration thisConf = new Configuration(conf);
      RCFileOutputFormat.setColumnNumber(thisConf, group.getColumns().size());
      writeConf.put(group.getName(), thisConf);
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
  
  public ColumnarSerDeBase getGroupSerDe(String groupName) {
    return serdes.get(groupName);
  }
}
