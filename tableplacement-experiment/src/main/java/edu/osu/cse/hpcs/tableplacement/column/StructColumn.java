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

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

@SuppressWarnings("rawtypes")
public class StructColumn extends Column<List> {

	public final static String INT_IN_STRUCT_RANGE_STR = "range.struct.int";
	public final static String DOUBLE_IN_STRUCT_RANGE_STR = "range.struct.double";
	public final static String STRING_IN_STRUCT_length_STR = "length.struct.string";

	public final static int DEFAULT_INT_IN_STRUCT_RANGE = 30000;
	public final static int DEFAULT_DOUBLE_IN_STRUCT_RANGE = 40000;
	public final static int DEFAULT_STRING_IN_STRUCT_LENGTH = 4;

	private RandomWrapper[] fieldRandoms;

	private ObjectInspector[] fieldObjectInspectors;

	private int numFields;

	public StructColumn(String name, String[] structFieldNames, String[] structFieldTypes,
      TableProperty prop) throws TablePropertyException {
    this.name = name;
    this.type = Column.Type.MAP;

    this.hiveObjectInspector = ObjectInspectorFactory
    	.getStandardStructObjectInspector(
    			Arrays.asList(structFieldNames),
    			Arrays.asList(fieldObjectInspectors));
  }

	@Override
	public List nextValue() {
		List<Object> ret = new ArrayList<Object>();
		for (int i=0; i<numFields; i++) {
			ret.add(fieldRandoms[i].nextValue());
		}
		return ret;
	}

	@Override
	public String toString() {
		return "Column[name:" + name + ", type:" + type + ", keyRandom: "
				+ keyRandom.toString() + ", valueRandom: "
				+ valueRandom.toString() + ", size: " + size + "]";
	}

	@Override
	public String nextValueAsString() {
		String ret = nextValue().toString();
		ret = ret.substring(1, ret.length() - 1);
		ret = ret.replace(", ", collectionDelim).replace("=", mapkeyDelim);
		return ret;
	}
}
