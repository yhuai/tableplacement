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
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestTableProperty extends TestBase {

  Logger log = Logger.getLogger(TestTableProperty.class);

  TableProperty testTableProperty;

  public TestTableProperty() throws URISyntaxException, IOException,
      TablePropertyException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("testTableProperty.properties");
    File file = new File(url.toURI());
    testTableProperty = new TableProperty(file);
  }

  @Test
  public void testLoadTableProperty() throws TablePropertyException {
    final String hiveColumnNames = "cint,cdouble,cstring,cmap1,cmap2,cmap3";
    final String hvieColumnTypes = "int:double:string:map<int,string>:map<string,string>:map<string,double>";
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

    List<Column> ret = testTableProperty.getColumns();

    Assert.assertEquals(expectedColumnStr.size(), ret.size());

    for (int i = 0; i < ret.size(); i++) {
      Assert.assertEquals(expectedColumnStr.get(i), ret.get(i).toString());
    }
  }
}
