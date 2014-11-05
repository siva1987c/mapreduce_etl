package com.xxx.algo.ad.mr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * LineRecordWriter wrapped in {@link TextOutputFormat}
 * 
 * @see https://issues.apache.org/jira/browse/MAPREDUCE-370
 * @see http://www.bodiya.info/archives/84
 * @see http://blog.csdn.net/inkfish/archive/2010/01/08/5156651.aspx
 * 
 */
public class LineRecordWriter<K, V> extends RecordWriter<K, V> {
	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	protected DataOutputStream out;
	private final byte[] keyValueSeparator;

	public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
		this.out = out;
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	public LineRecordWriter(DataOutputStream out) {
		this(out, "\t");
	}

	/**
	 * Write the object to the byte stream, handling Text as a special case.
	 * 
	 * @param o
	 *            the object to print
	 * @throws IOException
	 *             if the write throws, we pass it on
	 */
	private void writeObject(Object o) throws IOException {
		if (o instanceof Text) {
			Text to = (Text) o;
			out.write(to.getBytes(), 0, to.getLength());
		} else {
			out.write(o.toString().getBytes(utf8));
		}
	}

	public synchronized void write(K key, V value) throws IOException {

		boolean nullKey = key == null || key instanceof NullWritable;
		boolean nullValue = value == null || value instanceof NullWritable;
		if (nullKey && nullValue) {
			return;
		}
		if (!nullKey) {
			writeObject(key);
		}
		if (!(nullKey || nullValue)) {
			out.write(keyValueSeparator);
		}
		if (!nullValue) {
			writeObject(value);
		}
		out.write(newline);
	}

	public synchronized void close(TaskAttemptContext context)
			throws IOException {
		out.close();
	}
}
