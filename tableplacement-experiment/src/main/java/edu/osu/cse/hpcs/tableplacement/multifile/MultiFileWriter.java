package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.osu.cse.hpcs.tableplacement.ColumnFileGroup;
import edu.osu.cse.hpcs.tableplacement.TableProperty;

public abstract class MultiFileWriter {

  protected FileSystem fs;
  protected Configuration conf;
  protected Path outputDir;
  protected Path[] outputFiles;
  protected List<ColumnFileGroup> columnFileGroups;

  public MultiFileWriter(TableProperty prop, Configuration conf, Path outputDir)
      throws IOException {
    this.columnFileGroups = prop.getColumnFileGroups();
    this.outputDir = outputDir;
    this.fs = this.outputDir.getFileSystem(conf);
    this.outputFiles = new Path[this.columnFileGroups.size()];
    for (int i=0; i<this.columnFileGroups.size(); i++) {
      ColumnFileGroup group = columnFileGroups.get(i);
      outputFiles[i] = new Path(outputDir, group.getName());
    }
  }
}
