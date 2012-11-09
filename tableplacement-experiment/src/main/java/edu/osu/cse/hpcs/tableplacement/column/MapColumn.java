package edu.osu.cse.hpcs.tableplacement.column;

import java.util.HashMap;
import java.util.Map;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class MapColumn extends Column {

  public final String INT_IN_MAP_RANGE_STR = "range.map.int";
  public final String DOUBLE_IN_MAP_RANGE_STR = "range.map.double";
  public final String STRING_IN_MAP_length_STR = "length.map.string";
  public final String SIZE_MAP_STR = "size.map";

  public final int DEFAULT_INT_IN_MAP_RANGE = 30000;
  public final int DEFAULT_DOUBLE_IN_MAP_RANGE = 40000;
  public final int DEFAULT_STRING_IN_MAP_LENGTH = 4;
  public final int DEFAULT_SIZE_MAP = 5;

  private RandomWrapper keyRandom;
  private RandomWrapper valueRandom;

  private int size;

  public MapColumn(String name, String keyType, String valueType,
      TableProperty prop) throws TablePropertyException {
    this.name = name;
    this.type = Column.Type.MAP;

    if (DataType.INT_STR.equals(keyType)) {
      int range = prop.getInt(INT_IN_MAP_RANGE_STR, DEFAULT_INT_IN_MAP_RANGE);
      this.keyRandom = new IntRandom(range);
    } else if (DataType.DOUBLE_STR.equals(keyType)) {
      int range = prop.getInt(DOUBLE_IN_MAP_RANGE_STR,
          DEFAULT_DOUBLE_IN_MAP_RANGE);
      this.keyRandom = new DoubleRandom(range);
    } else if (DataType.STRING_STR.equals(keyType)) {
      int length = prop.getInt(STRING_IN_MAP_length_STR,
          DEFAULT_STRING_IN_MAP_LENGTH);
      this.keyRandom = new StringRandom(length);
    } else {
      throw new TablePropertyException("The key type " + keyType
          + " is not supported");
    }

    if (DataType.INT_STR.equals(valueType)) {
      int range = prop.getInt(INT_IN_MAP_RANGE_STR, DEFAULT_INT_IN_MAP_RANGE);
      this.valueRandom = new IntRandom(range);
    } else if (DataType.DOUBLE_STR.equals(valueType)) {
      int range = prop.getInt(DOUBLE_IN_MAP_RANGE_STR,
          DEFAULT_DOUBLE_IN_MAP_RANGE);
      this.valueRandom = new DoubleRandom(range);
    } else if (DataType.STRING_STR.equals(valueType)) {
      int length = prop.getInt(STRING_IN_MAP_length_STR,
          DEFAULT_STRING_IN_MAP_LENGTH);
      this.valueRandom = new StringRandom(length);
    } else {
      throw new TablePropertyException("The value type " + valueType
          + " is not supported");
    }

    this.size = prop.getInt(SIZE_MAP_STR, DEFAULT_SIZE_MAP);
  }

  @Override
  public Object nextValue() {
    Map<Object, Object> ret = new HashMap<Object, Object>();
    while (ret.size() < size) {
      ret.put(keyRandom.nextValue(), valueRandom.nextValue());
    }
    return ret;
  }

  @Override
  public String toString() {
    return "Column[name:" + name + ", type:" + type + ", keyRandom: "
        + keyRandom.toString() + ", valueRandom: " + valueRandom.toString()
        + ", size: " + size + "]";
  }
}
