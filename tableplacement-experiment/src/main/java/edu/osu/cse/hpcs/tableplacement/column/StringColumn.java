package edu.osu.cse.hpcs.tableplacement.column;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class StringColumn extends Column {

  public final String DOUBLE_LENGTH_STR = "length.string";
  public final int DEFAULT_STRING_LENGTH = 30;

  private StringRandom random;

  public StringColumn(String name, TableProperty prop) {
    this.name = name;
    this.type = Column.Type.STRING;
    int length = prop.getInt(DOUBLE_LENGTH_STR, DEFAULT_STRING_LENGTH);
    this.random = new StringRandom(length);
  }

  @Override
  public Object nextValue() {
    // Generate a random string with characters chosen from the set of
    // alpha-numeric characters
    return random.nextValue();
  }

  @Override
  public String toString() {
    return "Column[name:" + name + ", type:" + type + ", random: "
        + random.toString() + "]";
  }
}
