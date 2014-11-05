package com.xxx.algo.ad.lr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.xxx.algo.ad.lr.disc.utils.Feature;
import com.xxx.algo.ad.mr.*;

public class ModelPackage extends AbstractProcessor {

	public static class ModelPackageMapper extends
			Mapper<Object, Text, Text, Text> {

		boolean isFeature;
		int id;

		@Override
		public void setup(Context context) {
			String filePathString = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			System.err.println(filePathString);
			isFeature = false;
			if (filePathString.startsWith("p")) {
				filePathString = filePathString.split("-")[2];
				isFeature = true;
			}
			id = Integer.parseInt(filePathString);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (isFeature) {
				context.write(new Text(id + "\t1"), value);
			} else {
				context.write(new Text(id + "\t0"), value);
			}
		}
	}

	public static class ModelPackagePartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			// partitioned by psid
			return ((Integer.parseInt(key.toString().split("\t")[0]) % nr) + nr)
					% nr;
		}
	}

	public static class ModelPackageReducer extends
			Reducer<Text, Text, Text, Text> {

		private HashMap<Integer, Float> model_;
		private HashMap<String, HashMap<String, Feature>> idMap_;

		public class FeatureComparator implements Comparator<Feature> {

			@Override
			public int compare(Feature o1, Feature o2) {
				if (o1.weight < o2.weight)
					return -1;
				else if (o1.weight > o2.weight)
					return 1;
				else
					return 0;
			}
		}

		@Override
		public void setup(Context context) {
			model_ = new HashMap<Integer, Float>();
			idMap_ = new HashMap<String, HashMap<String, Feature>>();
		}

		private int getDefaultIdByMid(Set<SimpleEntry<String, Feature>> items) {
			long totalpv = 0;
			ArrayList<Feature> arr = new ArrayList<Feature>();
			for (SimpleEntry<String, Feature> entry : items) {
				Feature f = entry.getValue();
				arr.add(f);
				totalpv += f.pv;
			}
			System.out.println("item size=" + items.size() + ",arr size="
					+ arr.size());
			Collections.sort(arr, new FeatureComparator());
			long midpv = totalpv / 2;
			System.out.println("midpv=" + midpv + ",totalpv=" + totalpv);
			for (int i = 0; i < arr.size(); ++i) {
				midpv -= arr.get(i).pv;
				if (midpv <= 0) {
					return arr.get(i).id;
				}
			}
			System.out.println("[ERROR] get default id error with midpv="
					+ midpv);
			return 0;
		}

		private HashSet<SimpleEntry<String, Feature>> getSimpleEntrySet0(
				HashMap<String, Feature> map) {
			HashSet<SimpleEntry<String, Feature>> hashSet = new HashSet<SimpleEntry<String, Feature>>();
			for (Entry<String, Feature> entry : map.entrySet()) {
				hashSet.add(new SimpleEntry<String, Feature>(entry));
			}
			return hashSet;
		}

		private HashSet<SimpleEntry<String, HashMap<String, Feature>>> getSimpleEntrySet1(
				HashMap<String, HashMap<String, Feature>> map) {
			HashSet<SimpleEntry<String, HashMap<String, Feature>>> hashSet = new HashSet<SimpleEntry<String, HashMap<String, Feature>>>();
			for (Entry<String, HashMap<String, Feature>> entry : map.entrySet()) {
				hashSet.add(new SimpleEntry<String, HashMap<String, Feature>>(
						entry));
			}
			return hashSet;
		}

		private SimpleEntry<Set<SimpleEntry<String, Feature>>, String> fillDefault(
				String type, Set<SimpleEntry<String, Feature>> items,
				String splitChar, String route) {
			String firstKey = items.iterator().next().getKey();
			if (!firstKey.contains(splitChar + "")) {
				String defaultName = "";
				if (!splitChar.equals("#")) {
					if (firstKey.contains("@")) {
						defaultName = "default@1.000000";
					} else {
						defaultName = "default";
					}
					int defaultId = getDefaultIdByMid(items);
					Float w = model_.get(defaultId);
					if (w == null)
						w = 0f;
					items.add(new SimpleEntry<String, Feature>(defaultName,
							new Feature(type, defaultName, 0, defaultId, w)));
				}
				return new SimpleEntry<Set<SimpleEntry<String, Feature>>, String>(
						items, defaultName);
			}
			HashMap<String, HashMap<String, Feature>> map = new HashMap<String, HashMap<String, Feature>>();
			for (SimpleEntry<String, Feature> entry : items) {
				String[] ts = entry.getKey().split(splitChar);
				String part = entry.getKey().split(splitChar)[0];
				HashMap<String, Feature> inside = map.get(part);
				if (inside == null) {
					inside = new HashMap<String, Feature>();
				}
				StringBuilder sb = new StringBuilder();
				for (int i = 1; i < ts.length; ++i) {
					sb.append(ts[i]);
					if (i + 1 < ts.length) {
						sb.append(splitChar);
					}
				}
				inside.put(sb.toString(), entry.getValue());
				map.put(part, inside);
			}
			Set<SimpleEntry<String, Feature>> res = new HashSet<SimpleEntry<String, Feature>>();
			String defaultName = "";
			for (SimpleEntry<String, HashMap<String, Feature>> entry : getSimpleEntrySet1(map)) {
				SimpleEntry<Set<SimpleEntry<String, Feature>>, String> ret = fillDefault(
						type, getSimpleEntrySet0(entry.getValue()), "_", route
								+ entry.getKey() + splitChar);
				defaultName = ret.getValue();
				for (SimpleEntry<String, Feature> entryInside : ret.getKey()) {
					String key = entry.getKey() + splitChar
							+ entryInside.getKey();
					Feature f = entryInside.getValue();
					f.value = key.replace(
							CommonDataAndFunc.CTRL_A, "_PINPAI-CPC");
					res.add(new SimpleEntry<String, Feature>(key, f));
				}
			}
			defaultName = "default" + splitChar + defaultName;
			if (splitChar != "#") {
				int defaultId = getDefaultIdByMid(res);
				Float w = model_.get(defaultId);
				if (w == null)
					w = 0f;
				res.add(new SimpleEntry<String, Feature>(defaultName,
						new Feature(type, defaultName, 0, defaultId, w)));
			}
			return new SimpleEntry<Set<SimpleEntry<String, Feature>>, String>(
					res, defaultName);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int type = Integer.parseInt(key.toString().split("\t")[1]);
			if (type == 0) { // process model weight first
				for (Text value : values) {
					String[] ts = value.toString().split(":", -1);
					try {
						int x = Integer.parseInt(ts[0]);
						float y = Float.parseFloat(ts[1]);
						model_.put(x, y);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} else { // process feature map second
				System.err.println("model size=" + model_.size());
				for (Text value : values) {
					String[] ts = value.toString().split("\t");
					if (ts[1].contains("default")) {
						continue;
					}
					int id = Integer.parseInt(ts[7]);
					Float w = model_.get(id);
					if (w == null)
						w = 0f;
					Feature f = new Feature(ts[0], ts[1],
							Long.parseLong(ts[3]), id, w);
					HashMap<String, Feature> mapInside = idMap_.get(ts[0]);
					if (mapInside == null) {
						mapInside = new HashMap<String, Feature>();
					}
					mapInside.put(ts[1].replace("_PINPAI-CPC",
							CommonDataAndFunc.CTRL_A), f);
					idMap_.put(ts[0], mapInside);
				}
				System.err.println("id map size=" + idMap_.size());
				for (SimpleEntry<String, HashMap<String, Feature>> k : getSimpleEntrySet1(idMap_)) {
					Set<SimpleEntry<String, Feature>> items = fillDefault(
							k.getKey(), getSimpleEntrySet0(k.getValue()), "#",
							"").getKey();
					for (Entry<String, Feature> entry : items) {
						Feature f = entry.getValue();
						StringBuilder sb = new StringBuilder(f.name);
						sb.append("^");
						sb.append(f.value);
						sb.append(":");
						Float w = model_.get(f.id);
						if (w == null) {
							continue;
						}
						sb.append(w);
						context.write(new Text(sb.toString()), new Text(""));
					}
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {

		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(ModelPackageMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(ModelPackagePartitioner.class);

		job.setReducerClass(ModelPackageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
