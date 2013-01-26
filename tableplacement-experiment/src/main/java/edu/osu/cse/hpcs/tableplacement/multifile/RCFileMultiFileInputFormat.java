package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.MapWritable;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.util.TPMapWritable;

public class RCFileMultiFileInputFormat <K extends LongWritable, V extends TPMapWritable>
extends FileInputFormat<K, V> implements InputFormatChecker{

	public RCFileMultiFileInputFormat() {
		setMinSplitSize(SequenceFile.SYNC_INTERVAL);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public boolean validateInput(FileSystem fs, HiveConf conf,
			ArrayList<FileStatus> files) throws IOException {

		return true;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {

		reporter.setStatus(split.toString());
		try {
			return ((RecordReader<K, V>) (new RCFileMultiFileRecordReader<K,V>(job, (FileSplit) split)));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SerDeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TablePropertyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	/** Splits files returned by {@link #listStatus(JobConf)} when
	 * they're too big.*/
	@Override
	@SuppressWarnings("deprecation")
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {

		return Arrays.copyOfRange(super.getSplits(job, numSplits), 0, 1);
	}
}
