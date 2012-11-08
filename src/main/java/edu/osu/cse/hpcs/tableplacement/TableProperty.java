package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.log4j.Logger;

public class TableProperty {

  // General properties
  public final static String COLUMN_NAME_STR = "column.name.string";
  public final static String COLUMN_TYPE_STR = "column.type.string";
  public final static String HADOOP_IO_BUFFER_SIZE = "io.file.buffer.size";
  
  // RCFile properties
  public final static String RCFILE_ROWGROUP_SIZE_STR = RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR;
  
  Logger log = Logger.getLogger(TableProperty.class);
  
  private Properties prop;
    
  private TableProperty() {
    prop = new Properties();
  }
  
  /**
   * Load a table property from a file.
   * @throws IOException 
   */
  public TableProperty(File propsFile) throws IOException {
    this();
    log.info("Load table property file from " + propsFile.getPath());
    FileInputStream fis = new FileInputStream(propsFile);
    prop.load(fis);
    fis.close();
  }
  
}
