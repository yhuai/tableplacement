package edu.osu.cse.hpcs.tableplacement.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

@SuppressWarnings("rawtypes")
public class StructColumn extends Column<List> {

  public final static String INT_IN_STRUCT_RANGE_STR = "range.struct.int";
  public final static String DOUBLE_IN_STRUCT_RANGE_STR = "range.struct.double";
  public final static String STRING_IN_STRUCT_length_STR = "length.struct.string";
  public final static String FIELD_DELIM_STR = Constants.FIELD_DELIM;

  public final static int DEFAULT_INT_IN_STRUCT_RANGE = 30000;
  public final static int DEFAULT_DOUBLE_IN_STRUCT_RANGE = 40000;
  public final static int DEFAULT_STRING_IN_STRUCT_LENGTH = 4;
  public final static String DEFAULT_FIELD_DELIM = ",";

  private String[] fieldNames;
  private String[] fieldTypes;
  private RandomWrapper[] fieldRandoms;
  private ObjectInspector[] fieldObjectInspectors;
  private int numFields;

  private String fieldDelim;

  public StructColumn(String name, String[] structFieldNames,
      String[] structFieldTypes, TableProperty prop, TypeInfo typeInfo)
      throws TablePropertyException {
    super(typeInfo);
    this.name = name;
    this.type = Column.Type.STRUCT;

    if (structFieldNames.length != structFieldTypes.length) {
      throw new TablePropertyException(
          "The number of field names and that of field types" + "are not equal");
    }

    fieldNames = structFieldNames;
    fieldTypes = structFieldTypes;
    fieldRandoms = new RandomWrapper[structFieldNames.length];
    fieldObjectInspectors = new ObjectInspector[structFieldNames.length];
    numFields = fieldNames.length; 
    for (int i = 0; i < numFields; i++) {
      if (DataType.INT_STR.equals(fieldTypes[i])) {
        int range = prop.getInt(INT_IN_STRUCT_RANGE_STR,
            DEFAULT_INT_IN_STRUCT_RANGE);
        this.fieldRandoms[i] = new IntRandom(range);
        this.fieldObjectInspectors[i] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
      } else if (DataType.DOUBLE_STR.equals(fieldTypes[i])) {
        int range = prop.getInt(DOUBLE_IN_STRUCT_RANGE_STR,
            DEFAULT_DOUBLE_IN_STRUCT_RANGE);
        this.fieldRandoms[i] = new DoubleRandom(range);
        this.fieldObjectInspectors[i] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
      } else if (DataType.STRING_STR.equals(fieldTypes[i])) {
        int length = prop.getInt(STRING_IN_STRUCT_length_STR,
            DEFAULT_STRING_IN_STRUCT_LENGTH);
        this.fieldRandoms[i] = new StringRandom(length);
        this.fieldObjectInspectors[i] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      } else {
        throw new TablePropertyException("The field type "
            + structFieldTypes[i] + " is not supported");
      }
    }

    this.fieldDelim = prop.get(FIELD_DELIM_STR);
    if (fieldDelim == null) {
      fieldDelim = DEFAULT_FIELD_DELIM;
    }

    this.hiveObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(Arrays.asList(structFieldNames),
            Arrays.asList(fieldObjectInspectors));
  }

  @Override
  public List nextValue() {
    List<Object> ret = new ArrayList<Object>();
    for (int i = 0; i < numFields; i++) {
      ret.add(fieldRandoms[i].nextValue());
    }
    return ret;
  }

  @Override
  public String toString() {
    String ret = "Column[name:" + name;
    for (int i = 0; i < fieldNames.length; i++) {
      ret += ", Field[name:" + fieldNames[i] + ", type:" + fieldTypes[i] +
          ", random:" + fieldRandoms[i].toString() + "]";
    }
    return ret;
  }

  @Override
  public String nextValueAsString() {    
    String ret = nextValue().toString();
    ret = ret.substring(1, ret.length() - 1);
    return ret;
  }
}
