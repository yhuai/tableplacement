package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.column.DoubleColumn;
import edu.osu.cse.hpcs.tableplacement.column.IntColumn;
import edu.osu.cse.hpcs.tableplacement.column.MapColumn;
import edu.osu.cse.hpcs.tableplacement.column.StringColumn;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestTableProperty extends TestBase {

  Logger log = Logger.getLogger(TestTableProperty.class);

  TableProperty testTableProperty;

  public TestTableProperty() throws URISyntaxException, IOException,
      TablePropertyException {
    super();

  }

  @Test
  public void testLoadTableProperty() throws TablePropertyException,
      IOException, URISyntaxException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testTableProperty.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);

    final String hiveColumnNames = "cint,cdouble,cstring,cmap1,cmap2,cmap3,cstruct1";
    final String hvieColumnTypes = "int:double:string:map<int,string>:map<string,string>:map<string,double>:struct<fstring:string,fint:int,fdouble:double>";
    final int ioBufferSize = 524288;
    final int rcfileRowGroupSize = 4194304;

    List<String> expectedColumnStr = new ArrayList<String>();
    expectedColumnStr
        .add("Column[name:cint, type:INT, random: IntRandom[range=2147483647]]");
    expectedColumnStr
        .add("Column[name:cdouble, type:DOUBLE, random: DoubleRandom[range=100000]]");
    expectedColumnStr
        .add("Column[name:cstring, type:STRING, random: StringRandom[length=30]]");
    expectedColumnStr
        .add("Column[name:cmap1, type:MAP, keyRandom: IntRandom[range=65535], valueRandom: StringRandom[length=4], size: 10]");
    expectedColumnStr
        .add("Column[name:cmap2, type:MAP, keyRandom: StringRandom[length=4], valueRandom: StringRandom[length=4], size: 10]");
    expectedColumnStr
        .add("Column[name:cmap3, type:MAP, keyRandom: StringRandom[length=4], valueRandom: DoubleRandom[range=100000], size: 10]");
    expectedColumnStr
        .add("Column[name:cstruct1, Field[name:fstring, type:string, random:StringRandom[length=5]], Field[name:fint, type:int, random:IntRandom[range=100]], Field[name:fdouble, type:double, random:DoubleRandom[range=200]]");

    Assert.assertEquals(hiveColumnNames,
        testTableProperty.get(Constants.LIST_COLUMNS));
    Assert.assertEquals(hvieColumnTypes,
        testTableProperty.get(Constants.LIST_COLUMN_TYPES));
    Assert.assertEquals(ioBufferSize, testTableProperty.getInt(
        TableProperty.HADOOP_IO_BUFFER_SIZE,
        TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE));
    Assert.assertEquals(rcfileRowGroupSize, testTableProperty.getInt(
        TableProperty.RCFILE_ROWGROUP_SIZE_STR,
        TableProperty.DEFAULT_RCFILE_ROWGROUP_SIZE_STR));

    List<Column> ret = testTableProperty.getColumnList();

    Assert.assertEquals(expectedColumnStr.size(), ret.size());

    for (int i = 0; i < ret.size(); i++) {
      Assert.assertEquals(expectedColumnStr.get(i), ret.get(i).toString());
    }
  }

  @Test
  public void testLoadTablePropertyAndOverwrite()
      throws TablePropertyException, IOException, URISyntaxException {
    final String hiveColumnNames = "c1,c2,c3,c4,c5";
    final String hvieColumnTypes = "int,double,string,map<string,int>,map<string,double>";
    final int ioBufferSize = 262144;
    final int rcfileRowGroupSize = 16777216;

    // Overwrite properties
    Properties other = new Properties();
    other.setProperty(TableProperty.HADOOP_IO_BUFFER_SIZE,
        Integer.toString(ioBufferSize));
    other.setProperty(TableProperty.RCFILE_ROWGROUP_SIZE_STR,
        Integer.toString(rcfileRowGroupSize));
    other.setProperty(Constants.LIST_COLUMNS, hiveColumnNames);
    other.setProperty(Constants.LIST_COLUMN_TYPES, hvieColumnTypes);
    other.setProperty(IntColumn.INT_RANGE_STR, "10");
    other.setProperty(DoubleColumn.DOUBLE_RANGE_STR, "100");
    other.setProperty(StringColumn.STRING_LENGTH_STR, "50");
    other.setProperty(MapColumn.INT_IN_MAP_RANGE_STR, "999");
    other.setProperty(MapColumn.DOUBLE_IN_MAP_RANGE_STR, "9999");
    other.setProperty(MapColumn.STRING_IN_MAP_length_STR, "9");
    other.setProperty(MapColumn.SIZE_MAP_STR, "99");

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testTableProperty.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file, other);

    List<String> expectedColumnStr = new ArrayList<String>();
    expectedColumnStr
        .add("Column[name:c1, type:INT, random: IntRandom[range=10]]");
    expectedColumnStr
        .add("Column[name:c2, type:DOUBLE, random: DoubleRandom[range=100]]");
    expectedColumnStr
        .add("Column[name:c3, type:STRING, random: StringRandom[length=50]]");
    expectedColumnStr
        .add("Column[name:c4, type:MAP, keyRandom: StringRandom[length=9], valueRandom: IntRandom[range=999], size: 99]");
    expectedColumnStr
        .add("Column[name:c5, type:MAP, keyRandom: StringRandom[length=9], valueRandom: DoubleRandom[range=9999], size: 99]");

    Assert.assertEquals(hiveColumnNames,
        testTableProperty.get(Constants.LIST_COLUMNS));
    Assert.assertEquals(hvieColumnTypes,
        testTableProperty.get(Constants.LIST_COLUMN_TYPES));
    Assert.assertEquals(ioBufferSize, testTableProperty.getInt(
        TableProperty.HADOOP_IO_BUFFER_SIZE,
        TableProperty.DEFAULT_HADOOP_IO_BUFFER_SIZE));
    Assert.assertEquals(rcfileRowGroupSize, testTableProperty.getInt(
        TableProperty.RCFILE_ROWGROUP_SIZE_STR,
        TableProperty.DEFAULT_RCFILE_ROWGROUP_SIZE_STR));

    List<Column> ret = testTableProperty.getColumnList();

    Assert.assertEquals(expectedColumnStr.size(), ret.size());

    for (int i = 0; i < ret.size(); i++) {
      Assert.assertEquals(expectedColumnStr.get(i), ret.get(i).toString());
    }
  }
  
  @Test
  public void testColumnFileGroups() throws TablePropertyException, URISyntaxException, IOException {
    Map<String, String> expectedColumnStr = new LinkedHashMap<String, String>();
    expectedColumnStr
        .put("cint", "Column[name:cint, type:INT, random: IntRandom[range=2147483647]]");
    expectedColumnStr
        .put("cdouble", "Column[name:cdouble, type:DOUBLE, random: DoubleRandom[range=100000]]");
    expectedColumnStr
        .put("cstring", "Column[name:cstring, type:STRING, random: StringRandom[length=30]]");
    expectedColumnStr
        .put("cmap1", "Column[name:cmap1, type:MAP, keyRandom: IntRandom[range=65535], valueRandom: StringRandom[length=4], size: 10]");
    expectedColumnStr
        .put("cmap2", "Column[name:cmap2, type:MAP, keyRandom: StringRandom[length=4], valueRandom: StringRandom[length=4], size: 10]");
    expectedColumnStr
        .put("cmap3", "Column[name:cmap3, type:MAP, keyRandom: StringRandom[length=4], valueRandom: DoubleRandom[range=100000], size: 10]");
    expectedColumnStr
        .put("cstruct1", "Column[name:cstruct1, Field[name:fstring, type:string, random:StringRandom[length=5]], Field[name:fint, type:int, random:IntRandom[range=100]], Field[name:fdouble, type:double, random:DoubleRandom[range=200]]");
    
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testTableProperty.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);
    testTableProperty.set(TableProperty.COLUMN_FILE_GROUP,
        "cint,cmap3,cstruct1,cmap1,cmap2|cdouble,cstring");
    testTableProperty.prepareColumns(); // prepare columns and column file groups again
    List<ColumnFileGroup> columnFileGroups = testTableProperty.getColumnFileGroups();
    Assert.assertEquals(2, columnFileGroups.size());
    for (ColumnFileGroup group: columnFileGroups) {
      System.out.println(group.toString());
      for (Column column: group.getColumns()) {
        Assert.assertEquals(expectedColumnStr.get(column.getName()), column.toString());
      }
    }
    
    
  }
}
