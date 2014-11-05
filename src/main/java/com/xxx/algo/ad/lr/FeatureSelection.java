package com.xxx.algo.ad.lr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.xxx.algo.ad.lr.utils.*;
import com.xxx.algo.ad.mr.*;

public class FeatureSelection extends AbstractProcessor {

	public static class FeatureSelectionMapper extends
			Mapper<Object, Text, Text, Text> {

		Map<String, String> type_dict_; // feature => type
		Map<Integer, String> order_dict_; // line no.=> feature

		Set<String> use_dict_; // feature set after filtering
		Map<String, String> ngram_dict_; // feature union => union type
		Map<String, List<String>> base_union_dict_; // feature union => how to union
		Map<String, List<String>> test_union_dict_; // feature union => how to union

		Map<String, String> psids_; // psid => reducer id

		HashingMap<Float> model_; // featurehash => weight

		Map<String, Float> ps_rate_ = new HashMap<String, Float>(); // psid sampling rate
		
		Boolean processSpec = false;
		String fspec;
		
		private String ComputeLoss(double s, double y, double weight) {
			double p = 1. / (1. + Math.exp(-s));
			double l1 = (p - (y + 1) / 2) * weight;
			double l2 = p * (1 - p) * weight;
			return l1 + "\t" + l2;
		}

		public void setup(Context context) {
			// spec for parse hive data table
			String conf_file = context.getConfiguration().get("column_spec",
					"column.spec");
			type_dict_ = CommonDataAndFunc.readMaps(conf_file, "=", 0, 1,
					Definition._ENCODING);
			order_dict_ = CommonDataAndFunc.readOrderMap(conf_file, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load column_spec");

			// spec for make single feature to cross union features
			String use_conf = context.getConfiguration().get("use_spec",
					"use.spec");
			use_dict_ = CommonDataAndFunc.readSets(use_conf, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load use_spec");

			String ngram_conf = context.getConfiguration().get("ngram_spec",
					"ngram.spec");
			ngram_dict_ = CommonDataAndFunc.readMaps(ngram_conf, "=", 1, 0,
					Definition._ENCODING);
			System.err.println("success to load ngram_spec");

			// already used union feature & test feature waiting for selection
			base_union_dict_ = new HashMap<String, List<String>>();
			test_union_dict_ = new HashMap<String, List<String>>();
			for (String key : ngram_dict_.keySet()) {
				String[] ts = key.split("-");
				List<String> value = new ArrayList<String>(ts.length);
				for (int i = 0; i < ts.length; ++i)
					value.add(ts[i]);
				if (use_dict_.contains(key)) {
					base_union_dict_.put(key, value);
				} else {
					test_union_dict_.put(key, value);
				}
			}
			System.err.println("success to load make union dict");

			// ////////////////////////////////////////////////////////////////////////////
			String psid_conf = context.getConfiguration().get("psid_spec",
					"psid.spec");
			psids_ = CommonDataAndFunc.readMaps(psid_conf, " ", 0, 1,
					Definition._ENCODING);
			System.err.println("success to load make psid spec");

			// //////////////////////////////////////////////////////////////
			String model_conf = context.getConfiguration().get("model_spec",
					"model.spec");
			model_ = readModel(model_conf);
			System.err.println("success to load model");
			
			////////////////////////////////////////////////////////////////
			String sampling_conf = context.getConfiguration()
					.get("sampling_pv");
			if (sampling_conf != null) {
				float sampling_pv = Integer.parseInt(sampling_conf);
				System.out.println("read sampling pv = " + sampling_pv);

				Map<String, String> pvmap = CommonDataAndFunc.readMaps(
						psid_conf, " ", 0, 3, Definition._ENCODING);
				for (String key : pvmap.keySet()) {
					int pv = Integer.parseInt(pvmap.get(key));
					float rate = Math.min(1.0f, sampling_pv / pv);
					ps_rate_.put(key, rate);
				}

			} else {
				sampling_conf = context.getConfiguration().get("sampling_rate",
						"1.0");
				float sampling_rate = Float.parseFloat(sampling_conf);
				System.out.println("read sampling rate = " + sampling_rate);

				for (String key : psids_.keySet()) {
					ps_rate_.put(key, sampling_rate);
				}
			}
			
			////////////////////////////////////////////////////////////////
			
			fspec = context.getConfiguration().get("fspec");
			if (((FileSplit) context.getInputSplit()).getPath().toString()
					.endsWith(fspec)) {
				processSpec = true;
			}
		}

		public HashingMap<Float> readModel(String file_name) {
			HashingMap<Float> model = new HashingMap<Float>();
			try {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new FileInputStream(file_name)));
				String line = "";
				while ((line = reader.readLine()) != null) {
					if (line.startsWith("#"))
						continue;
					String[] terms = line.trim().split(":");
					if (terms.length <= 1 || terms[0].isEmpty()
							|| terms[1].isEmpty())
						continue;
					String key = terms[0];
					float value = Float.parseFloat(terms[1]);
					model.insert(key, value);
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return model;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (processSpec) {
				String psid = psids_.get(value.toString().split("\t")[1].split("#")[0]);
				if (psid != null) {
					context.write(new Text(psid + "@#"), value);
				}
				return;
			}
			
			String label = null;
			String psid = "";
			Float sum_w = 0.f;

			Map<String, String> wordmap = new HashMap<String, String>();
			Map<String, String> allmap = new HashMap<String, String>();

			String[] arr = value.toString().split(CommonDataAndFunc.CTRL_A, -1);
			for (int i = 0; i < arr.length; ++i) {
				if (arr[i].length() == 0 || arr[i].equals("\\N")) {
					System.err.println("unexpected missing value:"
							+ value.toString());
					return;
				}

				String name = order_dict_.get(i);
				if (name == null) {
					System.err.println("unexpected order " + i + " " + value);
					return;
				}

				if (name.equals(Definition._LABEL)) {
					label = arr[i];
					continue;
				}

				if (name.equals(Definition._PSID)) {
					psid = arr[i];
				}

				wordmap.put(name, arr[i]);
				if (use_dict_.contains(name)) {
					Float w = model_.getValue(name, arr[i]);
					if (w == null) {
						continue;
					}
					sum_w += w;
				}
			} // end for

			// check validation
			if (label == null)
				return;
			// filtering not interested psid
			if (!psids_.containsKey(psid))
				return;

			// union feature
			for (String ukey : base_union_dict_.keySet()) {
				String uv = "";
				boolean flag = true;
				List<String> values = base_union_dict_.get(ukey);
				for (String f : values) {
					String v = wordmap.get(f);
					if (v == null) {
						System.err.println("Failed to find " + f
								+ " for union:" + ukey);
						flag = false;
						break;
					}
					if (f.equals(Definition._PSID))
						uv = v + "#";
					else
						uv += v + "_";
				}
				if (flag) {
					int len = uv.length();
					uv = uv.substring(0, len - 1);
					allmap.put(ukey, uv);
				}
			}

			// feature value => weight
			for (String name : allmap.keySet()) {
				String fv = allmap.get(name);

				if (!model_.containsKey(name, fv)) {
					List<String> values = base_union_dict_.get(name);
					int end = values.size() - 1;
					while (end > 0) {
						String uv = "";
						for (int i = 0; i < end; ++i) {
							String f = values.get(i);
							String v = wordmap.get(f);
							if (f.equals(Definition._PSID))
								uv = v + "#";
							else
								uv += v + "_";
						}
						for (int i = end; i < values.size(); ++i) {
							uv += "default_";
						}

						uv = uv.substring(0, uv.length() - 1);
						if (model_.containsKey(name, uv)) {
							fv = uv;
							break;
						} else {
							end -= 1;
						}
					} // end while
				} // end if

				Float w = model_.getValue(name, fv);
				if (w == null) {
					System.err.println("Not found weight:" + name + "^" + fv);
				} else {
					sum_w += w;
				}
			}

			// for reverse sort
			double prediction = sum_w + Math.log(ps_rate_.get(psid));

			// calculate remain features not used
			allmap.clear();

			// union feature
			for (String ukey : test_union_dict_.keySet()) {
				String uv = "";
				boolean flag = true;
				List<String> values = test_union_dict_.get(ukey);
				for (String f : values) {
					String v = wordmap.get(f);
					if (v == null) {
						System.err.println("Failed to find " + f + " for test:"
								+ ukey);
						flag = false;
						break;
					}
					if (f.equals(Definition._PSID))
						uv = v + "#";
					else
						uv += v + "_";
				}
				if (flag) {
					int len = uv.length();
					uv = uv.substring(0, len - 1);
					allmap.put(ukey, uv);
				}
			}
			
			// psid header
			String header = psids_.get(psid);

			// feature value => weight
			String result = ComputeLoss(prediction, Integer.parseInt(label), 1);
			for (String name : allmap.keySet()) {
				String fv = allmap.get(name);
				//System.err.println(name + "====" + fv);
				context.write(new Text(header + "@" + name + "\t" + fv), new Text(result));
			}
		}

	}
	
	public static class FeatureSelectionPartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			String part = key.toString().split("@")[0];
			return Integer.parseInt(part) % nr;
		}
	}
	
	public static class FeatureSelectionCombiner extends
			Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (key.toString().endsWith("#")) {
				for (Text value: values) {
					context.write(key, value);
				}
				return;
			}
			double l1 = 0, l2 = 0;
			for (Text value : values) {
				String[] terms = value.toString().split("\t");
				l1 += Float.parseFloat(terms[0]);
				l2 += Float.parseFloat(terms[1]);
			}
			context.write(key, new Text(l1 + "\t" + l2));
		}
		
	}

	public static class FeatureSelectionReducer extends
			Reducer<Text, Text, Text, Text> {
		
		class L1L2 {
			public float l1 = 0;
			public float l2 = 0;
			public void add(float _l1, float _l2) {
				l1 += _l1;
				l2 += _l2;
			}
		}

		boolean flag = false;
		HashingMap<Integer> hash_dict_; // type => (value => id)
		HashMap<String, HashMap<String, L1L2>> data = new HashMap<String, HashMap<String, L1L2>>();
		
		@Override
		public void setup(Context context) {

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// System.err.println(key.toString());
			// process feature_id.spec
			// make sure its the first reduce key !!!
			if (key.toString().endsWith("#")) {
				if (flag) {
					System.err.println("not first pass!!!");
				}
				hash_dict_ = new HashingMap<Integer>();
				for (Text value : values) {
					String[] ts = value.toString().split("\t");
					int id = Integer.parseInt(ts[2]);
					// drop feature
					if (id < 0) {
						System.err.println("drop id:" + value.toString());
						continue;
					}
					// type, value, encoding_id
					hash_dict_.insert(ts[0], ts[1], Integer.parseInt(ts[2]));
				}
				System.err.println("success to load make feature dict===========" + hash_dict_.getState());
				return;
			}
			
			flag = true;
			
			// summary l1 and l2
			String[] words = key.toString().split("\t");
			String type = words[0];
			String id = words[1];

			float _l1 = 0, _l2 = 0;
			for (Text value : values) {
				String[] terms = value.toString().split("\t");
				_l1 += Float.parseFloat(terms[0]);
				_l2 += Float.parseFloat(terms[1]);
			}
			
			// put l1, l2 into counter
			Integer fid = hash_dict_.getValue(type.split("@")[1], id);
			if (fid == null) return;
			HashMap<String, L1L2> mapInside = data.get(type);
			if (mapInside == null) mapInside = new HashMap<String, L1L2>(); 
			L1L2 l1l2 = mapInside.get(fid);
			if (l1l2 == null) l1l2 = new L1L2();
			l1l2.add(_l1, _l2);
			mapInside.put(fid + "", l1l2);
			data.put(type, mapInside);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String key : data.keySet()) {
				HashMap<String, L1L2> mapInside = data.get(key);
				double s = 0;
				for (Entry<String, L1L2> id : mapInside.entrySet()) {
					L1L2 l1l2 = id.getValue();
					s += -0.5 * l1l2.l1 * l1l2.l1 / l1l2.l2;
				}
				context.write(new Text(key), new Text(s + ""));
			}
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(FeatureSelectionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(FeatureSelectionPartitioner.class);
		
		job.setCombinerClass(FeatureSelectionCombiner.class);

		job.setReducerClass(FeatureSelectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
