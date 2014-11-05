package com.xxx.algo.ad.lr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.xxx.algo.ad.mr.*;

public class BasicWordCount extends AbstractProcessor {

	public static class FeatureMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] label_and_sample = value.toString().split("\\|");
			String[] ts1 = label_and_sample[0].split(" ");
			String label = ts1[0];
			int w = Integer.parseInt(ts1[1]);
			String[] ts = label_and_sample[1].split(" ");
			for (int i = 0; i < ts.length; ++i) {
				if (ts[i].length() > 0)
					if (label.equals("1")) {
						context.write(new Text(ts[i]), new Text(w + "\t" + w));
					} else {
						context.write(new Text(ts[i]), new Text(w + "\t0"));
					}
			}
		}
	}

	public static class FeatureReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long pv = 0, clk = 0;
			for (Text value : values) {
				String[] ts = value.toString().split("\t");
				pv += Long.parseLong(ts[0]);
				clk += Long.parseLong(ts[1]);
			}
			context.write(key, new Text(pv + "\t" + clk));
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(FeatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(FeatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
