package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.column.DataType;
import edu.osu.cse.hpcs.tableplacement.column.DoubleColumn;
import edu.osu.cse.hpcs.tableplacement.column.IntColumn;
import edu.osu.cse.hpcs.tableplacement.column.MapColumn;
import edu.osu.cse.hpcs.tableplacement.column.StringColumn;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TableProperty {

  // General properties
  public final static String HADOOP_IO_BUFFER_SIZE = "io.file.buffer.size";

  // Default values of general properties
  public final static int DEFAULT_HADOOP_IO_BUFFER_SIZE = 131072; // 128KB

  // Hive properties
  // Refer to org.apache.hadoop.hive.serde.Constants

  // RCFile properties
  public final static String RCFILE_ROWGROUP_SIZE_STR = RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR;

  // Default values of RCFile properties
  public final static int DEFAULT_RCFILE_ROWGROUP_SIZE_STR = 16777216; // 16MB

  Logger log = Logger.getLogger(TableProperty.class);

  private Properties prop;
  private File propsFile;

  private TableProperty() {
    prop = new Properties();
  }

  /**
   * Load a table property from a file.
   * 
   * @throws IOException
   * @throws TablePropertyException
   */
  public TableProperty(File propsFile) throws IOException,
      TablePropertyException {
    this();
    this.propsFile = propsFile;
    log.info("Load table property file from " + propsFile.getPath());
    FileInputStream fis = new FileInputStream(propsFile);
    prop.load(fis);
    fis.close();
  }

  public void set(String key, String value) {
    prop.setProperty(key, value);
  }

  public String get(String key) {
    return prop.getProperty(key);
  }

  public void setInt(String key, int value) {
    prop.setProperty(key, Integer.toString(value));
  }

  public int getInt(String key, int defaultValue) {
    String valueString = prop.getProperty(key);
    if (valueString == null)
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public void setLong(String key, long value) {
    prop.setProperty(key, Long.toString(value));
  }

  public long getLong(String key, long defaultValue) {
    String valueString = prop.getProperty(key);
    if (valueString == null)
      return defaultValue;
    try {
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public void setBoolean(String key, boolean value) {
    prop.setProperty(key, Boolean.toString(value));
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String valueString = prop.getProperty(key);
    if (valueString == null)
      return defaultValue;
    return Boolean.parseBoolean(valueString);
  }

  public List<Column> getColumns() throws TablePropertyException {
    String columnsStr = get(Constants.LIST_COLUMNS);
    if (columnsStr == null || columnsStr.length() == 0) {
      throw new TablePropertyException("Columns (key: "
          + Constants.LIST_COLUMNS + ") are not defined in the property file "
          + propsFile.getPath());
    }
    List<String> columnNames = Arrays.asList(columnsStr.split(","));
    log.info("columnNames is " + columnNames);

    String columnsTypesStr = get(Constants.LIST_COLUMN_TYPES);
    if (columnsTypesStr == null || columnsTypesStr.length() == 0) {
      throw new TablePropertyException("Column types (key: "
          + Constants.LIST_COLUMN_TYPES
          + ") are not defined in the property file " + propsFile.getPath());
    }
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnsTypesStr);
    log.info("columnTypes is " + columnTypes);

    if (columnNames.size() != columnTypes.size()) {
      throw new TablePropertyException(
          "The number of column names and that of column types are not equal");
    }

    List<Column> columns = new ArrayList<Column>();

    for (int i = 0; i < columnNames.size(); i++) {
      String name = columnNames.get(i);
      TypeInfo type = columnTypes.get(i);

      if (DataType.INT_STR.equals(type.getTypeName())) {
        Column column = new IntColumn(name, this);
        columns.add(column);
      } else if (DataType.DOUBLE_STR.equals(type.getTypeName())) {
        Column column = new DoubleColumn(name, this);
        columns.add(column);
      } else if (DataType.STRING_STR.equals(type.getTypeName())) {
        Column column = new StringColumn(name, this);
        columns.add(column);
      } else if (type.getTypeName().startsWith(DataType.MAP_STR)) {
        String typeStr = type.getTypeName();
        String[] kv = typeStr.substring(4, typeStr.length() - 1).split(",");
        assert kv.length == 2;
        String keyType = kv[0];
        String valueType = kv[1];
        Column column = new MapColumn(name, keyType, valueType, this);
        columns.add(column);
      } else {
        throw new TablePropertyException("The type " + type.getTypeName()
            + " provided in " + this.propsFile.getPath() + " is not supported");
      }
    }

    return columns;
  }

  public void copyToHadoopConf(Configuration conf) {
    for (Entry<Object, Object> entry : prop.entrySet()) {
      conf.set((String) entry.getKey(), (String) entry.getValue());
    }
  }

}
