package edu.osu.cse.hpcs.tableplacement.multifile;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile.KeyBuffer;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import edu.osu.cse.hpcs.tableplacement.TableProperty;
import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.util.TPMapWritable;

public class RCFileMultiFileRecordReader <K extends LongWritable, V extends TPMapWritable>
implements RecordReader<LongWritable, TPMapWritable> {
	private final long start;
	private final long end; 
	private RCFileMultiFileReader reader; 
	protected Configuration conf;
	
	public RCFileMultiFileRecordReader(Configuration conf, 
			FileSplit split) throws IOException,
			ClassNotFoundException, SerDeException, InstantiationException,
			IllegalAccessException, TablePropertyException {

		Path inDir = split.getPath();
		String readColumns = conf.get("tableplacement.multifile.rcfile.readColumns");
		
		this.reader = new RCFileMultiFileReader(conf, inDir, readColumns);
		this.conf = conf;
		this.start = this.reader.getPos();
		this.end = split.getStart() + split.getLength();
	}

	public Class<?> getKeyClass() {
		return LongWritable.class;
	}

	public Class<?> getValueClass() {
		return TPMapWritable.class;
	}

	public LongWritable createKey() {
		return (LongWritable) ReflectionUtils.newInstance(getKeyClass(), conf);
	}

	public TPMapWritable createValue() {
		return (TPMapWritable) ReflectionUtils.newInstance(getValueClass(), conf);
	}
	
//semantics?
	public boolean nextBlock() throws IOException {
		return false; 
	}


	protected boolean next(LongWritable rowID) throws IOException {
		return reader.next(rowID);
	}
	
	@Override
	public boolean next(LongWritable key, TPMapWritable ret)
			throws IOException {
		boolean more = next(key);
		if (more)
			reader.getCurrentRow(ret);
		
		return more;
	}

	

	/**
	 * Return the progress within the input split.
	 *
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (reader.getPos() - start) / (float) (end - start));
		}
	}
	//sematic?
	public long getPos() throws IOException {
		return reader.getPos();
	}
	
	//sematic?
	protected void seek(long pos) throws IOException {
		reader.seek(pos);
	}
	//sematic?
	public void sync(long pos) throws IOException {
		reader.sync(pos);
	}

	public void resetBuffer() {
		reader.resetBuffer();
	}

	public long getStart() {
		return start;
	}

	public void close() throws IOException {
		reader.close();

	}
}
