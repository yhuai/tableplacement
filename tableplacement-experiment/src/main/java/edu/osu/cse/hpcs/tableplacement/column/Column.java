package edu.osu.cse.hpcs.tableplacement.column;

public abstract class Column<T> {

  public static enum Type {
    INT, DOUBLE, STRING, MAP
  }

  protected String name;
  protected Type type;

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
}
