package edu.osu.cse.hpcs.tableplacement.column;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public abstract class Column<T> {

  public static enum Type {
    INT, DOUBLE, STRING, MAP, STRUCT
  }

  protected String name;
  protected Type type;
  protected TypeInfo typeInfo;

  protected ObjectInspector hiveObjectInspector;

  protected Column(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  /**
   * @return next value
   */
  public abstract T nextValue();

  /**
   * @return next value as a string
   */
  public abstract String nextValueAsString();

  /**
   * @return the name of this column
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type of this column
   */
  public Type getType() {
    return type;
  }

  /**
   * @return the type of this column as a string
   */
  public String getTypeString() {
    return type.toString();
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }
  
  /**
   * @return the Hive object inspector of this column
   */
  public ObjectInspector getHiveObjectInspector() {
    return hiveObjectInspector;
  }

}
