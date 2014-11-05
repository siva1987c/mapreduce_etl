package com.xxx.algo.ad.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.sina.hadoop.rcfile.RCFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 *         This abstract class extends the FileOutputFormat, allowing to write
 *         the output data to different output files. This class is used for a
 *         map reduce job with at least one reducer. The reducer wants to write
 *         data to different files depending on the actual keys. It is assumed
 *         that a key (or value) encodes the actual key (value) and the desired
 *         location for the actual key (value).
 * 
 */

public abstract class MultipleRCFileOutputFormat<K extends WritableComparable, V extends BytesRefArrayWritable> 
	extends RCFileOutputFormat<WritableComparable, BytesRefArrayWritable> {

	public RecordWriter<WritableComparable, BytesRefArrayWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MultiRecordWriter(context, super.getOutputPath(context),
				super.getUniqueFile(context, "part", ""));
	}

	protected abstract  String generateFileNameForKeyValue(WritableComparable key, BytesRefArrayWritable value,
			String baseName) throws IOException, InterruptedException;
	protected abstract WritableComparable getActualKey(WritableComparable key);

	public class MultiRecordWriter extends RecordWriter<WritableComparable, BytesRefArrayWritable> {
		private HashMap<String, RecordWriter<WritableComparable, BytesRefArrayWritable>> recordWriters = null;
		private TaskAttemptContext context = null;
		private Path workPath = null;
		private String baseName = null;

		public MultiRecordWriter(TaskAttemptContext context, Path workPath,
				String baseName) {
			super();
			this.context = context;
			this.workPath = workPath;
			this.baseName = baseName;
			recordWriters = new HashMap<String, RecordWriter<WritableComparable, BytesRefArrayWritable>>();
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<RecordWriter<WritableComparable, BytesRefArrayWritable>> values 
				= this.recordWriters.values()
					.iterator();
			while (values.hasNext()) {
				values.next().close(context);
			}
			this.recordWriters.clear();
		}

		@Override
		public void write(WritableComparable key, BytesRefArrayWritable value) throws IOException, InterruptedException {
			String fileName = generateFileNameForKeyValue(key, value, baseName);
			RecordWriter<WritableComparable, BytesRefArrayWritable> rw = this.recordWriters.get(fileName);
			if (rw == null) {
				rw = getBaseRecordWriter(context, fileName);
				this.recordWriters.put(fileName, rw);
			}
			key = getActualKey(key);
			rw.write(key, value);
		}

		private RecordWriter<WritableComparable, BytesRefArrayWritable> getBaseRecordWriter(
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
						context, DefaultCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(
						codecClass, conf);
				extension = codec.getDefaultExtension();
			}
			Path file = new Path(workPath, fileName + extension);
			FileSystem fs = file.getFileSystem(conf);
			
		    final RCFile.Writer out = new RCFile.Writer(fs, conf, file, context, codec);

		    return new RecordWriter<WritableComparable, BytesRefArrayWritable>() {

		      @Override
		      public void close(TaskAttemptContext job) throws IOException {
		        out.close();
		      }

		      @Override
		      public void write(WritableComparable key, BytesRefArrayWritable value)
		          throws IOException {
		        out.append(value);
		      }
		    };
		}
	}
}
