package edu.osu.cse.hpcs.tableplacement.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;

import org.apache.hadoop.util.ReflectionUtils;

public class TPMapWritable extends AbstractMapWritable implements Map<LongWritable, BytesRefArrayWritable> {
	
	private Map<LongWritable, BytesRefArrayWritable> instance;

	/** Default constructor. */
	public TPMapWritable() {
		super();
		this.instance = new HashMap<LongWritable, BytesRefArrayWritable>();
	}

	/**
	 * Copy constructor.
	 * 
	 * @param other the map to copy from
	 */
	public TPMapWritable(MapWritable other) {
		this();
		copy(other);
	}

	/** {@inheritDoc} */
	public void clear() {
		instance.clear();
	}

	/** {@inheritDoc} */
	public boolean containsKey(Object key) {
		return instance.containsKey(key);
	}

	/** {@inheritDoc} */
	public boolean containsValue(Object value) {
		return instance.containsValue(value);
	}

	/** {@inheritDoc} */
	public Set<Map.Entry<LongWritable, BytesRefArrayWritable>> entrySet() {
		return instance.entrySet();
	}

	/** {@inheritDoc} */
	public BytesRefArrayWritable get(Object key) {
		return instance.get(key);
	}

	/** {@inheritDoc} */
	public boolean isEmpty() {
		return instance.isEmpty();
	}

	/** {@inheritDoc} */
	public Set<LongWritable> keySet() {
		return instance.keySet();
	}

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	public BytesRefArrayWritable put(LongWritable key, BytesRefArrayWritable value) {
		addToMap(key.getClass());
		addToMap(value.getClass());
		return instance.put(key, value);
	}

	/** {@inheritDoc} */
	public void putAll(Map<? extends LongWritable, ? extends BytesRefArrayWritable> t) {
		for (Map.Entry<? extends LongWritable, ? extends BytesRefArrayWritable> e: t.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	/** {@inheritDoc} */
	public BytesRefArrayWritable remove(Object key) {
		return instance.remove(key);
	}

	/** {@inheritDoc} */
	public int size() {
		return instance.size();
	}

	/** {@inheritDoc} */
	public Collection<BytesRefArrayWritable> values() {
		return instance.values();
	}

	// Writable

	/** {@inheritDoc} */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		// Write out the number of entries in the map

		out.writeInt(instance.size());

		// Then write out each key/value pair

		for (Map.Entry<LongWritable, BytesRefArrayWritable> e: instance.entrySet()) {
			out.writeByte(getId(e.getKey().getClass()));
			e.getKey().write(out);
			out.writeByte(getId(e.getValue().getClass()));
			e.getValue().write(out);
		}
	}

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		// First clear the map.  Otherwise we will just accumulate
		// entries every time this method is called.
		this.instance.clear();

		// Read the number of entries in the map

		int entries = in.readInt();

		// Then read each key/value pair

		for (int i = 0; i < entries; i++) {
			LongWritable key = (LongWritable) ReflectionUtils.newInstance(getClass(
					in.readByte()), getConf());

			key.readFields(in);

			BytesRefArrayWritable value = (BytesRefArrayWritable) ReflectionUtils.newInstance(getClass(
					in.readByte()), getConf());

			value.readFields(in);
			instance.put(key, value);
		}
	}


}
