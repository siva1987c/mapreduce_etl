package com.xxx.algo.ad.mr;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as offset in file and value as document record. Document records
 * should be separated using an entire line with explicit marks. The mark should
 * be defined using “doc.end.mark”.
 */
public class DocRecordReader extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(DocRecordReader.class
			.getName());

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private DocReader in;
	private int maxDocLength;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxDocLength = job.getInt("docrecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		final byte[] mark = job.get("doc.end.mark").getBytes();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstDoc = false;
		if (codec != null) {
			in = new DocReader(codec.createInputStream(fileIn), mark);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstDoc = true;
				start -= mark.length + 1;
				fileIn.seek(start);
			}
			in = new DocReader(fileIn, mark);
		}
		if (skipFirstDoc) { // skip first doc and re-establish "start".
			start += in.readDoc(new Text());
		}
		this.pos = start;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;

		while (pos < end) {
			newSize = in.readDoc(value);
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxDocLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

}
