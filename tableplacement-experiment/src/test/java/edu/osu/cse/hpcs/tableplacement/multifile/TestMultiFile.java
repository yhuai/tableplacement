package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.log4j.Logger;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.TestFormatBase;
import edu.osu.cse.hpcs.tableplacement.column.Column;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestMultiFile extends TestFormatBase {

  protected List<ColumnFileGroup> columnFileGroups;
  protected Map<String, StandardStructObjectInspector> groupOI;
  
  protected final int rowCount = 10;
  
  protected final String fullReadColumnStr =
      "cfg1:0,1|cfg2:0,1|cfg3:0";
  protected final String[] partialReadColumnStrs =
    {"cfg1:0", "cfg2:1", "cfg3:0", "cfg1:0|cfg2:0|cfg3:0"};
  
  public TestMultiFile(Logger log) throws URISyntaxException, IOException,
    TablePropertyException {
    super("testColumns.properties", "testRCFileMultiFile", log);
    testTableProperty.set(TableProperty.COLUMN_FILE_GROUP,
        "cfg1:cdouble,cint|cfg2:cstring,cstruct1|cfg3:cmap1");
    testTableProperty.prepareColumns();
    columnFileGroups = testTableProperty.getColumnFileGroups();
    groupOI = new HashMap<String, StandardStructObjectInspector>();
    for (ColumnFileGroup group: columnFileGroups) {
      groupOI.put(group.getName(), 
          (StandardStructObjectInspector)group.getGroupHiveObjectInspector());
    }
    localFS.mkdirs(path);
  }
  
  public List<Map<String, List<Object>>> genData(int rowCount) {
    List<Map<String, List<Object>>> ret =
        new ArrayList<Map<String, List<Object>>>();
    for (int i=0; i<rowCount; i++) {
      Map<String, List<Object>> row = new HashMap<String, List<Object>>();
      for (ColumnFileGroup columnFileGroup: columnFileGroups) {
        List<Object> thisGroup = new ArrayList<Object>();
        for (Column column: columnFileGroup.getColumns()){
          thisGroup.add(column.nextValue());
        }
        row.put(columnFileGroup.getName(), thisGroup);
      }
      ret.add(row);
    }
    return ret;
  }
}
