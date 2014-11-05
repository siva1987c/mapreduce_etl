package com.xxx.algo.ad.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link InputFormat} for multi-line document records. An entire line is
 * used to signal the border between records which should be defined using
 * "doc.end.mark". Keys are the position in the file, and values are the record
 * of text..
 */
public class DocInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new DocRecordReader();
	}
}
