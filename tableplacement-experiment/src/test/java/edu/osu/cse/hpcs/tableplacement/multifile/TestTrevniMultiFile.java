package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;

public class TestTrevniMultiFile {

  protected static Logger log = Logger.getLogger(TestTrevniMultiFile.class);

  @Test
  public void testTrevniMultiFile() throws SerDeException, InstantiationException,
      IllegalAccessException, IOException, ClassNotFoundException, TablePropertyException,
      URISyntaxException {
    Class[] classes = {ColumnarSerDe.class, LazyBinaryColumnarSerDe.class};
    for (int i=0; i<classes.length; i++) {
      MultiFileTestClass mftc = new MultiFileTestClass(
          classes[i],
          "testTrevniMultiFile", log);
      TrevniMultiFileWriter writer = mftc.getTrevniWriter();
      List<Map<String, List<Object>>> rows = mftc.writeData(writer);
      
      for (String groupName: writer.getSerializedSizeMapping().keySet()) {
        long[] sizes = writer.getSerializedSize(groupName);
        for (int j=0; j<sizes.length; j++) {
          System.out.println(groupName + " " + j + " " + sizes[j]);
        }
      }

      TrevniMultiFileReader fullReadReader = mftc.getTrevniReader(
          MultiFileTestClass.fullReadColumnStr);
      mftc.doFullMultiFileReadTest(rows, fullReadReader, 
          "test Trevni multi file full read with " +
              classes[i].getCanonicalName());
      
      for (int j=0; j<MultiFileTestClass.partialReadColumnStrs.length; j++) {
        String readColumnStr = MultiFileTestClass.partialReadColumnStrs[j];
        TrevniMultiFileReader partialReadReader = mftc.getTrevniReader(readColumnStr);
        mftc.doPartialMultiFileReadTest(rows, partialReadReader, 
            "test Trevni multi file partial read with " +
                classes[i].getCanonicalName() +
                ". Read columns: " + readColumnStr.toString());
      }
      
    }
  }
}
