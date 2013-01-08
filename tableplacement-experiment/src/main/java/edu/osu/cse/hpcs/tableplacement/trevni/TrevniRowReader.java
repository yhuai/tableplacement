package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;

public class TrevniRowReader {

  protected Logger log = Logger.getLogger(TrevniRowReader.class);

  private ColumnFileReader in;
  private int columnCount;
  private List<Integer> readCols;
  private int[] readColsArray;
  private ColumnValues<ByteBuffer>[] values;

  private long rowCount;

  public TrevniRowReader(ColumnFileReader in, int columnCount,
      List<Integer> readColsRef) throws IOException {
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
    for (int i = 0; i < values.length; i++) {
      values[i] = in.getValues(this.readCols.get(i));
      readColsArray[i] = this.readCols.get(i);
    }
    log.info("Total number of columns: " + columnCount);
    log.info("Read columns: " + this.readCols.toString());

    rowCount = 0;
  }

  public TrevniRowReader(ColumnFileReader in, int columnCount)
      throws IOException {
    this(in, columnCount, null);
  }

  public boolean next(LongWritable rowID) {
    if (values[0].hasNext()) {
      rowCount++;
      rowID.set(rowCount);
      return true;
    } else {
      return false;
    }
  }

  public void getCurrentRow(BytesRefArrayWritable braw) throws IOException {
    for (int i = 0; i < values.length; i++) {
      ByteBuffer v = values[i].next();
      BytesRefWritable ref = braw.unCheckedGet(readColsArray[i]);
      ref.set(v.array(), v.arrayOffset(), v.capacity());
    }
  }

}
