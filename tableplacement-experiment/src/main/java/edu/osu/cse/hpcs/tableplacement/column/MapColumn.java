package edu.osu.cse.hpcs.tableplacement.column;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde.Constants;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

@SuppressWarnings("rawtypes")
public class MapColumn extends Column<Map> {

  public final String INT_IN_MAP_RANGE_STR = "range.map.int";
  public final String DOUBLE_IN_MAP_RANGE_STR = "range.map.double";
  public final String STRING_IN_MAP_length_STR = "length.map.string";
  public final String SIZE_MAP_STR = "size.map";
  public final String COLLECTION_DELIM_STR = Constants.COLLECTION_DELIM;
  public final String MAPKEY_DELIM_STR = Constants.MAPKEY_DELIM;

  public final int DEFAULT_INT_IN_MAP_RANGE = 30000;
  public final int DEFAULT_DOUBLE_IN_MAP_RANGE = 40000;
  public final int DEFAULT_STRING_IN_MAP_LENGTH = 4;
  public final int DEFAULT_SIZE_MAP = 5;
  public final String DEFAULT_COLLECTION_DELIM = ",";
  public final String DEFAULT_MAPKEY_DELIM = "=";

  private RandomWrapper keyRandom;
  private RandomWrapper valueRandom;

  private int size;
  private String collectionDelim;
  private String mapkeyDelim;

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

    this.collectionDelim = prop.get(COLLECTION_DELIM_STR);
    if (collectionDelim == null) {
      collectionDelim = DEFAULT_COLLECTION_DELIM;
    }
    this.mapkeyDelim = prop.get(MAPKEY_DELIM_STR);
    if (mapkeyDelim == null) {
      mapkeyDelim = DEFAULT_MAPKEY_DELIM;
    }
  }

  @Override
  public Map nextValue() {
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

  @Override
  public String nextValueAsString() {
    String ret = nextValue().toString();
    ret = ret.substring(1, ret.length()-1);
    ret = ret.replace(", ", collectionDelim).replace("=", mapkeyDelim);
    return ret;
  }
}
