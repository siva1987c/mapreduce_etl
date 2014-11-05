package com.xxx.algo.ad.hive;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

import com.xxx.algo.ad.mr.*;
import com.sina.hadoop.rcfile.*;
import com.sina.hadoop.rcfile.utils.*;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;

public class WriteRCFilePartition extends AbstractProcessor {

	public static class PartitionMapper extends
			Mapper<LongWritable, BytesRefArrayWritable, LongWritable, BytesRefArrayWritable>{

		@Override
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}

	}


	public static class PartitionReducer extends
			Reducer<LongWritable, BytesRefArrayWritable, NullWritable, BytesRefArrayWritable> {
		
		@Override
		public void reduce(LongWritable key, Iterable<BytesRefArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (BytesRefArrayWritable value : values) {
				context.write(NullWritable.get(), value); 
			}
		}

	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(PartitionReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesRefArrayWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesRefArrayWritable.class);
		
		job.setInputFormatClass(RCFileInputFormat.class);
		job.setOutputFormatClass(PartitionMOF.class);
	}
}

class PartitionMOF extends MultipleRCFileOutputFormat<WritableComparable, BytesRefArrayWritable> {
	
	@Override
	protected String generateFileNameForKeyValue(WritableComparable key, BytesRefArrayWritable value,
			String baseName) throws IOException, InterruptedException {
		int length = value.size();
		String platform = "platform=" + LazyBinaryRCFileUtils.readString(value.get(length - 1));
		String idea = LazyBinaryRCFileUtils.readString(value.get(length - 2));
		if (idea.equals("text"))
			idea = "idea_type=text";
		else
			idea = "idea_type=image";
		return idea + "/" + platform + "/" + baseName;
	}
	
	@Override
	protected WritableComparable getActualKey(WritableComparable key) {
		// TODO Auto-generated method stub
		return key;
	}

}
