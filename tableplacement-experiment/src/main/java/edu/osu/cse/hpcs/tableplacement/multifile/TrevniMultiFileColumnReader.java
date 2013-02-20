package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.InputFile;
import org.apache.trevni.avro.HadoopInput;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.trevni.TrevniColumnReader;
import edu.osu.cse.hpcs.tableplacement.trevni.TrevniRowReader;
import edu.osu.cse.hpcs.tableplacement.util.Pair;
import edu.osu.cse.hpcs.tableplacement.util.TPMapWritable;

public class TrevniMultiFileColumnReader extends MultiFileReader<ColumnFileReader> {
  
  private static Logger log = Logger.getLogger(TrevniMultiFileColumnReader.class);

  private Map<String, TrevniColumnReader> columnReaders;
  private Iterator<Pair<String, Integer>> groupColsItr;
  private Pair<String, Integer> currentGrpCol = null;

  public TrevniMultiFileColumnReader(Configuration conf, Path inDir,
      String readColumnsStr, boolean isReadLocalFS) throws IOException,
      ClassNotFoundException, SerDeException, InstantiationException,
      IllegalAccessException, TablePropertyException {
    super(conf, inDir, readColumnsStr);
    columnReaders = new LinkedHashMap<String, TrevniColumnReader>();
    List<Pair<String, Integer>> groupCols= new ArrayList<Pair<String, Integer>>();
    
    for (String groupName: readColumns.keySet()) {
      ColumnFileGroup group = columnFileGroupsMap.get(groupName);
      Path file = columnFileGroupFiles.get(groupName);
      Configuration groupConf = readConf.get(groupName);
      ColumnFileReader reader;
      if (isReadLocalFS) {
        log.info("Local file system is used. " +
        		"Use org.apache.trevni.InputFile");
        String pathString = file.toUri().toString();
        if (pathString.startsWith("file:")) {
          pathString = pathString.substring("file:".length());
        }
        reader = new ColumnFileReader(new InputFile(new File(pathString)));
      } else {
        log.info("Distributed file system is used. " +
        		"Use org.apache.trevni.avro.HadoopInput.HadoopInput");
        log.info(TableProperty.HADOOP_IO_BUFFER_SIZE + ": " + groupConf.get(TableProperty.HADOOP_IO_BUFFER_SIZE));
        reader = new ColumnFileReader(new HadoopInput(file, groupConf));
      }
      readers.put(groupName, reader);
      List<Integer> readCols = readColumns.get(groupName);
      columnReaders.put(groupName,
          new TrevniColumnReader(reader, group.getColumns().size(), readCols));
      groupCols.add(new Pair(groupName, readCols));
    }
    groupColsItr = groupCols.iterator();
  }

  @Override
  public boolean next(LongWritable rowID) throws IOException {
    // do nothing
    return false;
  }

  @Override
  public boolean next(LongWritable rowID, String groupName, int column) {
    TrevniColumnReader reader = columnReaders.get(groupName);
    return reader.next(rowID, column);
  }
  
  public boolean nextColumn(LongWritable rowID) {
	  if (groupColsItr.hasNext()) {
		  Pair<String, Integer> e = groupColsItr.next();
		  this.currentGrpCol = e;
		  next(rowID, e.getL(), e.getR());
		  return true;
	  }
	  return false;
  }

  @Override
  public void getCurrentRow(Map<String, BytesRefArrayWritable> ret)
      throws IOException {
    // do nothing
  }

  @Override
  public void getCurrentColumnValue(Map<String, BytesRefArrayWritable> ret,
      String groupName, int column) throws IOException {
    TrevniColumnReader reader = columnReaders.get(groupName);
    BytesRefArrayWritable braw = ret.get(groupName);
    reader.getCurrentColumnValue(braw, column);
  }
  
  public void getCurrentColumnValue(TPMapWritable ret) 
		  throws IOException {
	  if (this.currentGrpCol != null) {
	    TrevniColumnReader reader = columnReaders.get(currentGrpCol.getL());
	    BytesRefArrayWritable braw = ret.get(currentGrpCol.getL());
	    reader.getCurrentColumnValue(braw, currentGrpCol.getR());
	  }
  }

  @Override
  public void close() throws IOException {
    for (ColumnFileReader reader: readers.values()) {
      reader.close();
    }
  }

}
