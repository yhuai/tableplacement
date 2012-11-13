package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.column.DoubleColumn;
import edu.osu.cse.hpcs.tableplacement.column.IntColumn;
import edu.osu.cse.hpcs.tableplacement.column.StringColumn;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestGenData extends TestBase {

  Logger log = Logger.getLogger(TestGenData.class);

  TableProperty testTableProperty;

  public TestGenData() throws URISyntaxException, IOException,
      TablePropertyException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testTableProperty.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testLoadTableProperty() throws TablePropertyException {
    List<Column> columns = testTableProperty.getColumns();

    for (Column col : columns) {
      System.out.println("Column name:" + col.getName() + ", Column type:"
          + col.getTypeString() + ", Column value:" + col.nextValue());
    }

    for (Column col : columns) {
      System.out.println("Column name:" + col.getName() + ", Column type:"
          + col.getTypeString() + ", Column value:" + col.nextValueAsString());
    }
  }

  @Test
  public void testIntColumnValueRange() {
    int range = 50000;
    IntColumn column = new IntColumn("int", range);
    int count = 10000;
    for (int i=0; i<count; i++) {
      Assert.assertTrue(column.nextValue() < range);
    }
  }

  @Test
  public void testDoubleColumnValueRange() {
    int range = 50000;
    DoubleColumn column = new DoubleColumn("double", range);
    int count = 10000;
    for (int i=0; i<count; i++) {
      Assert.assertTrue(column.nextValue() < range);
    }
  }
  
  @Test
  public void testStringColumnValueLength() {
    int length = 50;
    StringColumn column = new StringColumn("string", length);
    int count = 10000;
    for (int i=0; i<count; i++) {
      Assert.assertTrue(column.nextValue().length() == length);
    }
  }
}
