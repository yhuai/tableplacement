package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestRCFileMultiFile {

  protected static Logger log = Logger.getLogger(TestRCFileMultiFile.class);

  @Test
  public void testRCFileMultiFile() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException, ClassNotFoundException, TablePropertyException,
      URISyntaxException {
    Class[] classes = {ColumnarSerDe.class, LazyBinaryColumnarSerDe.class};
    for (int i=0; i<classes.length; i++) {
      MultiFileTestClass mftc = new MultiFileTestClass(
          classes[i],
          "testRCFileMultiFile", log);
      RCFileMultiFileWriter writer = mftc.getRCFileWriter();
      List<Map<String, List<Object>>> rows = mftc.writeData(writer);
      
      
      for (String groupName: writer.getSerializedSizeMapping().keySet()) {
        long[] sizes = writer.getSerializedSize(groupName);
        for (int j=0; j<sizes.length; j++) {
          System.out.println(groupName + " " + j + " " + sizes[j]);
        }
      }
      
      RCFileMultiFileReader fullReadReader = mftc.getRCFileReader(
          MultiFileTestClass.fullReadColumnStr);
      mftc.doFullMultiFileReadTest(rows, fullReadReader, 
          "test RCFile multi file full read with " +
              classes[i].getCanonicalName());
      
      for (int j=0; j<MultiFileTestClass.partialReadColumnStrs.length; j++) {
        String readColumnsStr = MultiFileTestClass.partialReadColumnStrs[j];
        RCFileMultiFileReader partialReadReader = mftc.getRCFileReader(readColumnsStr);
        mftc.doPartialMultiFileReadTest(rows, partialReadReader, 
            "test RCFile multi file partial read with " +
                classes[i].getCanonicalName() +
                ". Read columns: " + readColumnsStr.toString());
      }
    }
  }
}
