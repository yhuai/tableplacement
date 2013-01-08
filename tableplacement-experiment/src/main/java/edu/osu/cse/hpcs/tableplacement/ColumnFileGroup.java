package edu.osu.cse.hpcs.tableplacement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class ColumnFileGroup {
  private String name; // the name of this group
  private List<Column> columns;
  private List<String> columnNames;
  private List<ObjectInspector> columnObjectInspectors;
  private ObjectInspector groupHiveObjectInspector;
  private TableProperty groupProp;
  
  public ColumnFileGroup(String name, List<Column> columns, TableProperty baseProp)
      throws TablePropertyException {
    this.name = name;
    this.columns = columns;
    this.columnNames = new ArrayList<String>(columns.size());
    this.columnObjectInspectors = new ArrayList<ObjectInspector>(columns.size());
    for (Column column: columns) {
      columnNames.add(column.getName());
      columnObjectInspectors.add(column.getHiveObjectInspector());
    }
    this.groupHiveObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames,
            columnObjectInspectors);
    groupProp = new TableProperty(baseProp.getProperties());
    
    Column column = this.columns.get(0);
    String columnNameProperty = column.getName();
    String columnTypeProperty = column.getTypeInfo().getTypeName();
    for (int i=1; i<columns.size(); i++) {
      column = this.columns.get(i);
      columnNameProperty += "," + column.getName();
      columnTypeProperty += ":" + column.getTypeInfo().getTypeName();
    }
    groupProp.set(Constants.LIST_COLUMNS, columnNameProperty);
    groupProp.set(Constants.LIST_COLUMN_TYPES, columnTypeProperty);
    groupProp.remove(TableProperty.COLUMN_FILE_GROUP);
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

  public TableProperty getGroupProp() {
    return groupProp;
  }

  @Override
  public String toString() {
    return "Column file group:" + columns.toString();
  }
}
