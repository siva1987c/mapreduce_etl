package com.sina.algo.hive;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import my.hadoop.common.AbstractProcessor;
import my.hadoop.common.CommonDataAndFunc;

import org.apache.hadoop.io.NullWritable;
import my.hadoop.common.MultipleOutputFormat;

public class WritePartition extends AbstractProcessor {

	public static class PartitionMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String key_str = key.toString();
			context.write(new Text(key_str), value);
		}

	}


	public static class PartitionReducer extends
			Reducer<Text, Text, NullWritable, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(NullWritable.get(), value); 
			}
		}

	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(PartitionMapper.class);
		job.setOutputFormatClass(PartitionMOF.class);
		job.setReducerClass(PartitionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
	}
}

class PartitionMOF extends MultipleOutputFormat<NullWritable, Text> {
	
	@Override
	protected String generateFileNameForKeyValue(NullWritable key, Text value,
			String baseName) {
		String values[] = value.toString().split(CommonDataAndFunc.CTRL_A, -1);
		int length = values.length;
		String platform = "platform=" + values[length - 1];
		String idea = values[length - 2];
		if (idea.equals("text"))
			idea = "idea_type=text";
		else
			idea = "idea_type=image";
		return idea + "/" + platform + "/" + baseName;
		
	}

}