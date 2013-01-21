package edu.osu.cse.hpcs.tableplacement.trevni;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;

/**
 * Read a file in the format of Trevni column by column
 *
 */
public class TrevniColumnReader extends TrevniValueReader {
  protected static Logger log = Logger.getLogger(TrevniColumnReader.class);
  protected long[] rowCounts;

  public TrevniColumnReader(ColumnFileReader in, int columnCount,
      List<Integer> readColsRef) throws IOException {
    super(in, columnCount, readColsRef, log);
    rowCounts = new long[columnCount];
    for (int i=0; i<rowCounts.length; i++) {
      rowCounts[i] = 0;
    }
  }

  public TrevniColumnReader(ColumnFileReader in, int columnCount)
      throws IOException {
    this(in, columnCount, null);
  }
  
  public boolean next(LongWritable rowID, int col) {
    if (!readColsSet.contains(col)) {
      return false;
    }
    if (values[col].hasNext()) {
      rowCounts[col]++;
      rowID.set(rowCounts[col]);
      return true;
    } else {
      return false;
    }
  }

  /**
   * get the current value of column col and store it in the ith
   * position of braw. Must call {@link next} first.
   * @param braw
   * @param col
   * @throws IOException
   */
  public void getCurrentColumnValue(BytesRefArrayWritable braw, int col)
      throws IOException {
    ByteBuffer v = values[col].next();
    braw.resetValid(columnCount);
    BytesRefWritable ref = braw.unCheckedGet(col);
    ref.set(v.array(), v.arrayOffset(), v.capacity());
  }
}
