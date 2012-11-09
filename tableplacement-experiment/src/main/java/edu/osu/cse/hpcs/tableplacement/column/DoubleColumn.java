package edu.osu.cse.hpcs.tableplacement.column;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class DoubleColumn extends Column<Double> {

  public final String DOUBLE_RANGE_STR = "range.double";
  public final int DEFAULT_DOUBLE_RANGE = 20000;

  private DoubleRandom random;

  public DoubleColumn(String name, TableProperty prop) {
    this.name = name;
    this.type = Column.Type.DOUBLE;
    int range = prop.getInt(DOUBLE_RANGE_STR, DEFAULT_DOUBLE_RANGE);
    this.random = new DoubleRandom(range);
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
