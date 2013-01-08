package edu.osu.cse.hpcs.tableplacement.column;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class DoubleColumn extends Column<Double> {

  public final static String DOUBLE_RANGE_STR = "range.double";
  public final static int DEFAULT_DOUBLE_RANGE = 20000;

  private DoubleRandom random;

  public DoubleColumn(String name, TableProperty prop, TypeInfo typeInfo) {
    this(name, prop.getInt(DOUBLE_RANGE_STR, DEFAULT_DOUBLE_RANGE), typeInfo);
  }

  public DoubleColumn(String name, int range, TypeInfo typeInfo) {
    super(typeInfo);
    this.name = name;
    this.type = Column.Type.DOUBLE;
    this.random = new DoubleRandom(range);
    this.hiveObjectInspector = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  @Override
  public Double nextValue() {
    return random.nextValue();
  }

  @Override
  public String toString() {
    return "Column[name:" + name + ", type:" + type + ", random: "
        + random.toString() + "]";
  }

  @Override
  public String nextValueAsString() {
    return Double.toString(nextValue());
  }
}
