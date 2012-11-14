package edu.osu.cse.hpcs.tableplacement.column;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class IntColumn extends Column<Integer> {

  public final static String INT_RANGE_STR = "range.int";
  public final static int DEFAULT_INT_RANGE = 10000;

  private IntRandom random;

  public IntColumn(String name, TableProperty prop) {
    this(name, prop.getInt(INT_RANGE_STR, DEFAULT_INT_RANGE));
  }

  public IntColumn(String name, int range) {
    this.name = name;
    this.type = Column.Type.INT;
    this.random = new IntRandom(range);
    this.hiveObjectInspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  }

  @Override
  public Integer nextValue() {
    return random.nextValue();
  }

  @Override
  public String toString() {
    return "Column[name:" + name + ", type:" + type + ", random: "
        + random.toString() + "]";
  }

  @Override
  public String nextValueAsString() {
    return Integer.toString(nextValue());
  }

}
