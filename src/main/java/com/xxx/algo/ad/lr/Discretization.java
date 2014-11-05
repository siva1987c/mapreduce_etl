package com.xxx.algo.ad.lr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.xxx.algo.ad.mr.*;
import com.xxx.algo.ad.lr.disc.*;
import com.xxx.algo.ad.lr.disc.utils.*;
import com.xxx.algo.ad.lr.utils.Definition;

public class Discretization extends AbstractProcessor {

	public static class DiscretizationMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, Float> psidCtr; // PDPS id list, do not filter while list is empty
		@Override
		public void setup(Context context) {
			String psid_spec = context.getConfiguration().get("ps_spec", ""); // ps_spec configuration data can be none
			Map<String, String> pMap = new HashMap<String, String>();
			if (!psid_spec.equals("")) {
				pMap = CommonDataAndFunc.readMaps(psid_spec, " ", 0, 5,
						Definition._ENCODING);
			}
			psidCtr = new HashMap<String, Float>();
			for (Entry<String, String> entry : pMap.entrySet()) {
				psidCtr.put(entry.getKey(), Float.parseFloat(entry.getValue()));
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// filter by psidList while list is not empty
			String[] ts = value.toString().split("\t");
			if (psidCtr.size() == 0
					|| psidCtr.containsKey(ts[0].split("\\^")[1].split("#")[0])) {
				context.write(new Text(ts[0]), new Text(ts[1] + "\t" + ts[2]));
			}
		}
	}

	public static class DiscretizationPartitioner extends
			Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			// partitioned by feature type
			return ((key.toString().split("\\^")[0].hashCode() % nr) + nr) % nr;
		}
	}

	public static class DiscretizationGroupingComparator extends
			WritableComparator {
		// grouping by feature type and psid

		public DiscretizationGroupingComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable o1, WritableComparable o2) {
			String o1str = ((Text) o1).toString();
			String o2str = ((Text) o2).toString();

			String[] ts1 = o1str.split("\\^");
			String[] ts2 = o2str.split("\\^");

			String type1 = ts1[0];
			String type2 = ts2[0];

			String[] ps1 = ts1[1].split("#");
			String[] ps2 = ts2[1].split("#");

			String psid1 = ps1[0];
			String psid2 = ps2[0];

			int ret = type1.compareTo(type2);
			if (ret != 0 || (ps1.length == 1 && ps2.length == 1)) {
				return ret;
			} else {
				return psid1.compareTo(psid2);
			}
		}
	}

	public static class DiscretizationReducer extends
			Reducer<Text, Text, Text, Text> {

		private AtomicInteger unique = new AtomicInteger(0);
		private boolean useHash = false;
		private int clkThreshold;
		private HashMap<String, Float> psidCtr;
		private MultipleOutputs<Text, Text> mos;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			useHash = (context.getConfiguration().get("disc_type", "nohash")
					.equals("hash"));
			String psid_spec = context.getConfiguration().get("ps_spec", "");
			Map<String, String> pMap = CommonDataAndFunc.readMaps(psid_spec,
					" ", 0, 5, Definition._ENCODING);
			psidCtr = new HashMap<String, Float>();
			for (Entry<String, String> entry : pMap.entrySet()) {
				psidCtr.put(entry.getKey(), Float.parseFloat(entry.getValue()));
			}
			clkThreshold = context.getConfiguration().getInt("clknum", 3);
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Feature> fmap = new HashMap<String, Feature>();
			for (Text value : values) {
				String[] ts = value.toString().split("\t");
				long pv = Long.parseLong(ts[0]);
				long clk = Long.parseLong(ts[1]);

				String[] ks = context.getCurrentKey().toString().split("\\^");
				String curtName = ks[0];
				String curtKey = ks[1];

				Feature f = null;
				if (fmap.containsKey(curtKey)) {
					fmap.get(curtKey).update(pv, clk);
				} else {
					f = new Feature();
					f.set(curtName, curtKey, pv, clk);
					fmap.put(curtKey, f);
				}
			}

			ArrayList<String> result = indexing(fmap);
			Collections.sort(result);
			for (String t : result) {
				if (t.length() > 1) {
					mos.write("feature", new Text(t), new Text(""));
					if (t.startsWith("ps\t")) {
						mos.write("psid", new Text(t), new Text(""));
					}
				}
			}
		}

		private ArrayList<String> indexing(HashMap<String, Feature> fmap) {
			ArrayList<String> result = new ArrayList<String>();
			FeatureCluster fc;
			if (useHash) {
				fc = new HashFeatureCluster(unique);
			} else {
				fc = new HierarchyFeatureCluster(unique, psidCtr, clkThreshold);
			}
			for (Entry<String, Feature> entry : fc.CC(fmap).entrySet()) {
				Feature f = entry.getValue();
				result.add(String.format("%s\t%s\t%s\t%s\t%s\t%.6f\t%s\t%s",
						f.name, f.value, f.attr, f.pv, f.clk, f.clk
								/ (f.pv + 0.0001), 0.5, f.id));
			}
			if (result.size() <= 1) {
				unique.decrementAndGet();
			}
			return result;
		}
		
		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(DiscretizationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(DiscretizationPartitioner.class);

		job.setGroupingComparatorClass(DiscretizationGroupingComparator.class);

		job.setReducerClass(DiscretizationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "feature", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "psid", TextOutputFormat.class, Text.class, Text.class);
	}
}
