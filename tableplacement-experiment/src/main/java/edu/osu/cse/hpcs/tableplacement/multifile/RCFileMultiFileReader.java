package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;

import edu.osu.cse.hpcs.tableplacement.TableProperty;

public class RCFileMultiFileReader extends MultiFileReader<RCFile.Reader> {

  public RCFileMultiFileReader(TableProperty prop, Configuration conf,
      Path inDir, Map<String, List<Integer>> readColumns) throws IOException {
    super(prop, conf, inDir, readColumns);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean next(LongWritable rowID) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean next(LongWritable rowID, String groupName, int column) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void getCurrentRow(BytesRefArrayWritable ret) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void getValue(BytesRefArrayWritable ret) {
    // TODO Auto-generated method stub
    
  }

}
