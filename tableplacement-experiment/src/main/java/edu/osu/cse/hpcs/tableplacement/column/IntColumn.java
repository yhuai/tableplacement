package edu.osu.cse.hpcs.tableplacement.column;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class IntColumn extends Column {

  public final String INT_RANGE_STR = "range.int";
  public final int DEFAULT_INT_RANGE = 10000;

  private IntRandom random;

  public IntColumn(String name, TableProperty prop) {
    this.name = name;
    this.type = Column.Type.INT;
    int range = prop.getInt(INT_RANGE_STR, DEFAULT_INT_RANGE);
    this.random = new IntRandom(range);
  }

  @Override
  public Object nextValue() {
    return random.nextValue();
  }

  @Override
  public String toString() {
    return "Column[name:" + name + ", type:" + type + ", random: "
        + random.toString() + "]";
  }

}
