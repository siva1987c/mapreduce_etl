package com.xxx.algo.ad.lr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import com.xxx.algo.ad.mr.*;
import com.xxx.algo.ad.lr.utils.Definition;

public class DiscreteFormatting extends AbstractProcessor {

	public static class DiscreteFormattingMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Map<String, String> psids_;
		String seperateChar; // feature seperated by char, to collect psid

		@Override
		public void setup(Context context) {
			String psid_conf = context.getConfiguration().get("psid_spec",
					"psid.spec");
			psids_ = CommonDataAndFunc.readMaps(psid_conf, " ", 0, 1,
					Definition._ENCODING);
			seperateChar = context.getConfiguration().get("sep_char", "#");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// filter by psidList while list is not empty
			String[] ts = value.toString().split("\t");
			if (ts.length < 8) {
				System.err.println("[ERROR] feature length error");
				return;
			}
			String psid = ts[1].split(seperateChar)[0];
			String id = psids_.get(psid);
			if (id == null) { // this psid's data needn't to process
				return;
			}
			context.write(new Text(id + "\t" + ts[0]), value);
		}
	}

	public static class DiscretizationPartitioner extends
			Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			// partitioned by psid
			return ((Integer.parseInt(key.toString().split("\t")[0]) % nr) + nr)
					% nr;
		}
	}

	public static class DiscreteFormattingReducer extends
			Reducer<Text, Text, Text, Text> {

		private int nowId = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int maxId = 0, minId = 1000000000;
			boolean empty = true;
			List<String> cache = new ArrayList<String>();
			for (Text value : values) {
				String[] ts = value.toString().split("\t", -1);
				int id = Integer.parseInt(ts[7]);
				cache.add(value.toString());
				if (id < 0) { // drop feature
					continue;
				}
				if (empty) { // identify exist valid data
					empty = false;
				}
				maxId = (id > maxId) ? id : maxId;
				minId = (id < minId) ? id : minId;
			}
			if (empty) { // if empty or no valid data, correct the min id value
				minId = nowId;
			}
			for (String line : cache) {
				String[] ts = line.split("\t");
				int id = Integer.parseInt(ts[7]);
				ts[7] = (nowId + id - minId) + ""; // format into [nowId..nowId
													// + (maxId - minId)]
				
				context.write(new Text(StringUtils.join(ts, "\t")),
						new Text(""));
			}
			System.err.println(nowId);
			nowId += (maxId - minId + 1); // update nowId
			System.err.println("key=" + key.toString() + ", minId = " + minId
					+ ", maxId = " + maxId + "totalId=" + (maxId - minId + 1));
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(DiscreteFormattingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(DiscretizationPartitioner.class);

		job.setReducerClass(DiscreteFormattingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
