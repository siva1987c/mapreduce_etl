package com.xxx.algo.ad.mr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * To solve bug MAPREDUCE-370 in Hadoop V0.20.2
 * 
 * @see https://issues.apache.org/jira/browse/MAPREDUCE-370
 * @see http://www.bodiya.info/archives/84
 * @see http://blog.csdn.net/inkfish/archive/2010/01/08/5156651.aspx
 * 
 *         This abstract class extends the FileOutputFormat, allowing to write
 *         the output data to different output files. This class is used for a
 *         map reduce job with at least one reducer. The reducer wants to write
 *         data to different files depending on the actual keys. It is assumed
 *         that a key (or value) encodes the actual key (value) and the desired
 *         location for the actual key (value).
 * 
 */

public abstract class MultipleFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MultiRecordWriter(context, super.getOutputPath(context),
				super.getUniqueFile(context, "part", ""));
	}

	protected abstract String generateFileNameForKeyValue(K key, V value,
			String baseName);

	public class MultiRecordWriter extends RecordWriter<K, V> {
		private HashMap<String, RecordWriter<K, V>> recordWriters = null;
		private TaskAttemptContext context = null;
		private Path workPath = null;
		private String baseName = null;

		public MultiRecordWriter(TaskAttemptContext context, Path workPath,
				String baseName) {
			super();
			this.context = context;
			this.workPath = workPath;
			this.baseName = baseName;
			recordWriters = new HashMap<String, RecordWriter<K, V>>();
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<RecordWriter<K, V>> values = this.recordWriters.values()
					.iterator();
			while (values.hasNext()) {
				values.next().close(context);
			}
			this.recordWriters.clear();
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			String fileName = generateFileNameForKeyValue(key, value, baseName);
			RecordWriter<K, V> rw = this.recordWriters.get(fileName);
			if (rw == null) {
				rw = getBaseRecordWriter(context, fileName);
				this.recordWriters.put(fileName, rw);
			}
			rw.write(key, value);
		}

		private RecordWriter<K, V> getBaseRecordWriter(
				TaskAttemptContext context, String fileName)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			boolean isCompressed = getCompressOutput(context);
			String keyValueSeparator = conf.get(
					"mapred.multipleoutputformat.separator", "\t");

			CompressionCodec codec = null;
			String extension = "";
			if (isCompressed) {
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
						context, GzipCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(
						codecClass, conf);
				extension = codec.getDefaultExtension();
			}
			Path file = new Path(workPath, fileName + extension);
			FileSystem fs = file.getFileSystem(conf);
			if (!isCompressed) {
				FSDataOutputStream fileOut = fs.create(file, false);
				return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
			} else {
				FSDataOutputStream fileOut = fs.create(file, false);
				return new LineRecordWriter<K, V>(new DataOutputStream(
						codec.createOutputStream(fileOut)), keyValueSeparator);
			}
		}
	}
}
