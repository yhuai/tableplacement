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

public class TrevniRowReader extends TrevniValueReader {

  protected static Logger log = Logger.getLogger(TrevniRowReader.class);

  public TrevniRowReader(ColumnFileReader in, int columnCount,
      List<Integer> readColsRef) throws IOException {
    super(in, columnCount, readColsRef, log);
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

  /**
   * get values of current and store those values in the corresponding
   * position in braw. For those columns which are not needed,
   * those positions in braw are unset. Must call {@link next} first
   * @param braw
   * @throws IOException
   */
  public void getCurrentRow(BytesRefArrayWritable braw) throws IOException {
    for (int i = 0; i < values.length; i++) {
      values[i].startRowWithPrefetch(16);
      ByteBuffer v = values[i].nextValue();
      //ByteBuffer v = values[i].next();
      braw.resetValid(columnCount);
      BytesRefWritable ref = braw.unCheckedGet(readColsArray[i]);
      ref.set(v.array(), v.arrayOffset(), v.capacity());
    }
  }

}
