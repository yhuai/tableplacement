package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.column.DataType;
import edu.osu.cse.hpcs.tableplacement.column.DoubleColumn;
import edu.osu.cse.hpcs.tableplacement.column.IntColumn;
import edu.osu.cse.hpcs.tableplacement.column.MapColumn;
import edu.osu.cse.hpcs.tableplacement.column.StringColumn;
import edu.osu.cse.hpcs.tableplacement.column.StructColumn;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TableProperty {

  // General properties
  public final static String HADOOP_IO_BUFFER_SIZE = "io.file.buffer.size";
  public final static String READ_ALL_COLUMNS_STR = "all";
  public final static String READ_COLUMN_STR = "read.column.string";
  public final static String READ_COLUMN_MULTI_FILE_STR = "read.column.multi.file.string";
  // In the case that a table is stored in multiple files
  // and each of the file store a subset of columns.
  // This string representing which columns are in the same file.
  public final static String COLUMN_FILE_GROUP = "columns.file.group";

  // Default values of general properties
  public final static int DEFAULT_HADOOP_IO_BUFFER_SIZE = 131072; // 128KB

  // Hive properties
  // Refer to org.apache.hadoop.hive.serde.Constants
  public final static String SERDE_CLASS = "serde.class";

  // DEFAULT Hive properties
  public final static String DEFAULT_SERDE_CLASS = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.ColumnarSerDe";

  // RCFile properties
  public final static String RCFILE_ROWGROUP_SIZE_STR = RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR;

  // Default values of RCFile properties
  public final static int DEFAULT_RCFILE_ROWGROUP_SIZE_STR = 16777216; // 16MB
  public final static String DEFAULT_RCFILE_READ_COLUMN_STR = READ_ALL_COLUMNS_STR; // all
                                                                                    // columns

  Logger log = Logger.getLogger(TableProperty.class);

  private Properties prop;
  private File propsFile;

  private List<Column> columnList;
  private Map<String, Column> columns;
  private List<String> columnNames;
  private List<ColumnFileGroup> columnFileGroups;
  private List<ObjectInspector> columnHiveObjectInspectors;
  private ObjectInspector rowHiveObjectInspector;

  private TableProperty() {
    prop = new Properties();
  }
  
  public TableProperty(Properties other) {
    this();
    if (other != null) {
      prop.putAll(other);
    }
  }
  
  public TableProperty(Configuration conf) {
    this();
    for (Entry<String, String> entry: conf) {
      set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Load a table property from a file.
   * 
   * @param propsFile
   *          the table property file
   * @throws IOException
   * @throws TablePropertyException
   */
  public TableProperty(File propsFile) throws IOException,
      TablePropertyException {
    this(propsFile, null);
  }

  /**
   * @param propsFile
   *          the table property file
   * @param other
   *          all properties needed to be overwritten
   * @throws IOException
   * @throws TablePropertyException
   */
  public TableProperty(File propsFile, Properties other) throws IOException,
      TablePropertyException {
    this();
    this.propsFile = propsFile;
    log.info("Load table property file from " + propsFile.getPath());
    FileInputStream fis = new FileInputStream(propsFile);
    prop.load(fis);
    fis.close();
    if (other != null) {
      prop.putAll(other);
    }
  }

  public void prepareColumns() throws TablePropertyException {
    genColumns();
    genHiveObjectInspectors();
    this.rowHiveObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames,
            columnHiveObjectInspectors);
    
    // get which columns are in the same file
    columnFileGroups = new ArrayList<ColumnFileGroup>();
    String columnFileGroupsStr = get(COLUMN_FILE_GROUP);
    if (columnFileGroupsStr != null) {
      String[] groups = columnFileGroupsStr.split("\\|");
      for (int i=0; i<groups.length; i++) {
        List<Column> group = new ArrayList<Column>();
        String[] tmp = groups[i].split(":");
        assert tmp.length == 2; // a name of the group and a string for columns
        String groupName = tmp[0];
        String[] colNames = tmp[1].split(",");
        for (int j=0; j<colNames.length; j++) {
          if (columns.containsKey(colNames[j])) {
            group.add(columns.get(colNames[j]));
          } else {
            throw new TablePropertyException("Column " + colNames[j] +
                " has not declared in the property file");
          }
        }
        columnFileGroups.add(new ColumnFileGroup(groupName, group, this));
      }
    } else { // all columns are in the same file
      columnFileGroups.add(new ColumnFileGroup("all", columnList, this));
    }
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

  private void genColumns() throws TablePropertyException {
    String columnsStr = get(Constants.LIST_COLUMNS);
    if (columnsStr == null || columnsStr.length() == 0) {
      throw new TablePropertyException("Columns (key: "
          + Constants.LIST_COLUMNS + ") are not defined in the property file "
          + propsFile.getPath());
    }
    columnNames = Arrays.asList(columnsStr.split(","));
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

    columnList = new ArrayList<Column>();
    columns = new LinkedHashMap<String, Column>();
    for (int i = 0; i < columnNames.size(); i++) {
      String name = columnNames.get(i);
      TypeInfo type = columnTypes.get(i);

      if (DataType.INT_STR.equals(type.getTypeName())) {
        Column column = new IntColumn(name, this, type);
        columnList.add(column);
        columns.put(column.getName(), column);
      } else if (DataType.DOUBLE_STR.equals(type.getTypeName())) {
        Column column = new DoubleColumn(name, this, type);
        columnList.add(column);
        columns.put(column.getName(), column);
      } else if (DataType.STRING_STR.equals(type.getTypeName())) {
        Column column = new StringColumn(name, this, type);
        columnList.add(column);
        columns.put(column.getName(), column);
      } else if (type.getTypeName().startsWith(DataType.MAP_STR)) {
        String typeStr = type.getTypeName();
        String[] kv = typeStr.substring(4, typeStr.length() - 1).split(",");
        assert kv.length == 2;
        String keyType = kv[0];
        String valueType = kv[1];
        Column column = new MapColumn(name, keyType, valueType, this, type);
        columnList.add(column);
        columns.put(column.getName(), column);
      } else if (type.getTypeName().startsWith(DataType.STRUCT_STR)) {
        // Currently, Map type cannot be a field of a struct
        StructTypeInfo structType = (StructTypeInfo)type;
        String[] fieldNames = structType.getAllStructFieldNames().toArray(new String[0]);
        TypeInfo[] fieldTypes = structType.getAllStructFieldTypeInfos().toArray(new TypeInfo[0]);
        String[] fieldTypeStrs = new String[fieldTypes.length];
        for (int j=0; j<fieldTypes.length; j++) {
          fieldTypeStrs[j] = fieldTypes[j].getTypeName();
        }
        Column column = new StructColumn(name, fieldNames, fieldTypeStrs, this, type);
        columnList.add(column);
        columns.put(column.getName(), column);
      } else {
        throw new TablePropertyException("The type " + type.getTypeName()
            + " provided in " + this.propsFile.getPath() + " is not supported");
      }
    }
  }

  private void genHiveObjectInspectors() {
    columnHiveObjectInspectors = new ArrayList<ObjectInspector>(columnList.size());
    for (Column col : columnList) {
      columnHiveObjectInspectors.add(col.getHiveObjectInspector());
    }
  }

  public List<ObjectInspector> getHiveObjectInspectors() {
    return columnHiveObjectInspectors;
  }

  public ObjectInspector getHiveRowObjectInspector() {
    return rowHiveObjectInspector;
  }

  public List<Column> getColumnList() throws TablePropertyException {
    return columnList;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<ColumnFileGroup> getColumnFileGroups() {
    return columnFileGroups;
  }
  
  public void copyToHadoopConf(Configuration conf) {
    for (Entry<Object, Object> entry : prop.entrySet()) {
      conf.set((String) entry.getKey(), (String) entry.getValue());
    }
  }
  
  public String remove(String key) {
    return (String)prop.remove(key);
  }

  public Properties getProperties() {
    return prop;
  }

  public void dump() {
    System.out.println("Dump all table properties");
    for (Entry<Object, Object> entry : prop.entrySet()) {
      System.out.println((String) entry.getKey() + ": "
          + (String) entry.getValue());
    }
  }

}
