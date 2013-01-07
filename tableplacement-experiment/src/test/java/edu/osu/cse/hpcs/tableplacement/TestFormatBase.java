package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestFormatBase extends TestBase {

  protected TableProperty testTableProperty;
  protected Configuration hadoopConf;
  protected FileSystem localFS;
  protected Path file;
  protected ColumnarSerDeBase serde;

  protected List<Column> columns;
  protected final int columnCount;

  public TestFormatBase(String propertyFile, String testDataFile, Logger log)
      throws URISyntaxException, IOException, TablePropertyException {
    super();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource(propertyFile);
    testTableProperty = new TableProperty(new File(url.toURI()));
    columns = testTableProperty.getColumnList();
    columnCount = columns.size();
    hadoopConf = new Configuration();
    testTableProperty.copyToHadoopConf(hadoopConf);
    localFS = FileSystem.getLocal(hadoopConf);
    file = new Path(resourceDir, testDataFile);
    if (localFS.exists(file)) {
      log.info(file.getName() + " already exists in " + file.getParent()
          + ". Delete it first.");
      localFS.delete(file, true);
    }
  }

  public static List<List<Object>> getTest4ColRows(final int rowCount,
      final int mapSize) {
    List<List<Object>> ret = new ArrayList<List<Object>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      Map<String, Integer> map = new HashMap<String, Integer>();
      for (int j = 0; j < mapSize; j++) {
        map.put("r" + i + "m" + j, i * 10 + j);
      }
      List<Object> row = new ArrayList<Object>(4);
      row.add(i * 100);
      row.add(i * 100 + i * 0.001);
      row.add("r" + i);
      row.add(map);
      List<Object> struct = new ArrayList<Object>(3);
      struct.add("f" + i);
      struct.add(i * 101);
      struct.add(i * 101 + i * 0.001);
      row.add(struct);
      ret.add(row);
    }

    return ret;
  }

}
