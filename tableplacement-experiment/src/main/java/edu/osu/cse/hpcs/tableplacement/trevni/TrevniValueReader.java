package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;

public class TrevniValueReader {

  protected Logger log;

  protected ColumnFileReader in;
  protected int columnCount;
  protected List<Integer> readCols;
  protected int[] readColsArray;
  protected HashMap<Integer, Integer> columnMapping;
  protected HashSet<Integer> readColsSet;
  protected ColumnValues<ByteBuffer>[] values;
  protected long rowCount;

  public TrevniValueReader(ColumnFileReader in, int columnCount,
      List<Integer> readColsRef, Logger log) throws IOException {
    this.in = in;
    this.columnCount = columnCount;
    if (readColsRef == null) {
      this.readCols = new ArrayList<Integer>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        this.readCols.add(i);
      }
    } else {
      this.readCols = readColsRef;
    }
    values = new ColumnValues[this.readCols.size()];
    readColsArray = new int[this.readCols.size()];
    readColsSet = new HashSet<Integer>();
    columnMapping = new HashMap<Integer, Integer>();
    for (int i = 0; i < values.length; i++) {
      readColsArray[i] = this.readCols.get(i);
      values[i] = in.getValues(readColsArray[i]);
      readColsSet.add(readColsArray[i]);
      columnMapping.put(readColsArray[i], i);
    }
    this.log = log;
    this.log.info("Total number of columns: " + columnCount);
    this.log.info("Read columns: " + this.readCols.toString());
    
    rowCount = 0;
  }
}
