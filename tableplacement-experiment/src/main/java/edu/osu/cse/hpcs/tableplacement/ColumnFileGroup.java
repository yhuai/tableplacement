package edu.osu.cse.hpcs.tableplacement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import edu.osu.cse.hpcs.tableplacement.column.Column;

public class ColumnFileGroup {
  private String name; // the name of this group
  private List<Column> columns;
  private List<String> columnNames;
  private List<ObjectInspector> columnObjectInspectors;
  private ObjectInspector groupHiveObjectInspector;
  
  public ColumnFileGroup(String name, List<Column> columns) {
    this.name = name;
    this.columns = columns;
    this.columnNames = new ArrayList<String>(columns.size());
    this.columnObjectInspectors = new ArrayList<ObjectInspector>(columns.size());
    for (Column column: columns) {
      columnObjectInspectors.add(column.getHiveObjectInspector());
    }
    this.groupHiveObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames,
            columnObjectInspectors);
  }

  public String getName() {
    return name;
  }
  
  public List<Column> getColumns() {
    return columns;
  }

  public ObjectInspector getGroupHiveObjectInspector() {
    return groupHiveObjectInspector;
  }
  
  @Override
  public String toString() {
    return "Column file group:" + columns.toString();
  }
}
