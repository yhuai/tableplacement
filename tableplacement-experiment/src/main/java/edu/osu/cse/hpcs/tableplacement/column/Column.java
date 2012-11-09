package edu.osu.cse.hpcs.tableplacement.column;

public abstract class Column {

  public static enum Type {
    INT, DOUBLE, STRING, MAP
  }

  protected String name;
  protected Type type;

  public abstract Object nextValue();
}
