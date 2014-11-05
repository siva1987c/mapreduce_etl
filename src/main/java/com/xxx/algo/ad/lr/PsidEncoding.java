package com.xxx.algo.ad.lr;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.xxx.algo.ad.lr.utils.Definition;
import com.xxx.algo.ad.lr.utils.HashingMap;
import com.xxx.algo.ad.mr.*;

public class PsidEncoding extends AbstractProcessor {

	public static class EncodingMapper extends Mapper<LongWritable, Text, Text, Text> {

		Map<String, String> psids_; // psid => reducer id
		Map<Integer, String> order_dict_; // line no.=> feature
		Map<String, Float> ps_rate_ = new HashMap<String, Float>(); // psid =>
																	// sampling
																	// ratio

		String fspec;
		Boolean processSpec = false;

		Boolean randseq_ = true;
		final private static HashMap<String, Random> r = new HashMap<String, Random>();
		final private static Random rInt = new Random();

		public void setup(Context context) {
			String conf_file = context.getConfiguration().get("column_spec",
					"column.spec");
			order_dict_ = CommonDataAndFunc.readOrderMap(conf_file, "=", 0,
					Definition._ENCODING);
			String psid_conf = context.getConfiguration().get("psid_spec",
					"psid.spec");
			fspec = context.getConfiguration().get("fspec");
			// check if this mapper is feature id or label data
			if (((FileSplit) context.getInputSplit()).getPath().toString()
					.contains(fspec)) {
				processSpec = true;
			}
			psids_ = CommonDataAndFunc.readMaps(psid_conf, " ", 0, 1,
					Definition._ENCODING);
			System.err.println("success to load make psid spec");

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
					System.out.println(key + "=====" + rate);
					r.put(key, new Random());
				}
			} else {
				sampling_conf = context.getConfiguration().get("sampling_rate",
						"1.0");
				float sampling_rate = Float.parseFloat(sampling_conf);
				System.out.println("read sampling rate = " + sampling_rate);

				for (String key : psids_.keySet()) {
					ps_rate_.put(key, sampling_rate);
					r.put(key, new Random());
				}
			}
			String randseq_conf = context.getConfiguration().get("randseq",
					"true");
			randseq_ = Boolean.parseBoolean(randseq_conf);
		}

		private void processETL(Text data, Context context)
				throws IOException, InterruptedException {
			String label = null;
			String psid = "";
			String[] arr = StringUtils.split(data.toString(), CommonDataAndFunc.CTRL_A, -1);
			if (arr.length != order_dict_.size()) {
				System.err.println(data.toString());
				System.err.println("LOG LENGTH ERROR:" + arr.length + " vs " + order_dict_.size());
				return;
			}
			for (int i = 0; i < arr.length; ++i) {
				if (arr[i].length() == 0 || arr[i].equals("\\N")) {
					System.err.println("unexpected missing value:" + data);
					return;
				}
				String name = order_dict_.get(i);
				if (name.equals(Definition._LABEL)) {
					label = arr[i];
					continue;
				}
				if (name.equals(Definition._PSID)) {
					psid = arr[i];
				}
				if (name.equals(Definition._LIPV)) {
					int pv = 0;
					try {
						pv = Integer.parseInt(arr[i]);
					} catch (Exception e) {
						System.err.println("lipv error==========" + pv);
						return;
					}
					if (pv >= Definition._MAX_LIPV) {
						return;
					}
				}

			} // end for

			// check validation
			if (label == null)
				return;
			// filtering psid which is not interested
			if (!psids_.containsKey(psid))
				return;

			// psid header
			String header = psids_.get(psid);
			// random sequence
			if (randseq_) {
				header += "@"
						+ String.format("%010d%010d",
								Math.abs(rInt.nextInt()) % 1000000000,
								Math.abs(rInt.nextInt()) % 1000000000);
			}
			String result = label + " | " + data;
			// sampling rate and output
			float rate = ps_rate_.get(psid);
			if (label.equals("-1") && r.get(psid).nextDouble() > rate) {
				// System.err.println("drop by sampling");
				return;
			} else {
				result = header + "@ | " + result;
				// psid [@rand123randn123] @ | label | data
				context.write(new Text(result), new Text("1"));
			}
		}

		private void processFeature(Text data, Context context)
				throws IOException, InterruptedException {
			String[] terms = StringUtils.split(data.toString(), "\t");
			if (terms.length < 2) {
				System.err.println("FEATURE LENGTH ERROR");
				return;
			}
			// get psid in formatted features
			String psid = psids_.get(terms[1].split("#")[0]);

			// filtering psid which is not interested
			if (psid == null)
				return;

			// followed by "@#" is to make feature spec the first element
			// when go into the reducing sort process
			context.write(new Text(psid + "@#"), new Text(data.toString()));
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (processSpec) {
				processFeature(value, context);
			} else {
				processETL(value, context);
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
		}

	}

	public static class EncodingPartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			String[] ts = key.toString().split("@");
			String part = ts[0];
			return Integer.parseInt(part) % nr;
		}
	}

	public static class EncodingCombiner extends
			Reducer<Text, Text, Text, Text> {

		Boolean randseq_ = true;

		public void setup(Context context) {
			String randseq_conf = context.getConfiguration().get("randseq",
					"true");
			randseq_ = Boolean.parseBoolean(randseq_conf);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (randseq_ || key.toString().endsWith("#")) {
				for (Text value : values) {
					context.write(key, value);
				}
			} else {
				int sum = 0;
				for (Text cnt : values) {
					sum += Integer.parseInt(cnt.toString());
				}
				context.write(new Text(key), new Text(sum + ""));
			}
		}
	}

	public static class EncodingReducer extends Reducer<Text, Text, Text, Text> {

		Map<String, String> type_dict_; // feature => type
		Map<Integer, String> order_dict_; // line no.=> feature

		Set<String> use_dict_; // feature set after filtering

		Map<String, String> ngram_dict_; // feature union => union type
		Map<String, List<String>> union_dict_; // feature union => how to union
		HashingMap<Integer> hash_dict_; // type => (value => id)
		String feature_conf;
		boolean flag = false;

		public void setup(Context context) throws IOException {
			String conf_file = context.getConfiguration().get("column_spec",
					"column.spec");
			type_dict_ = CommonDataAndFunc.readMaps(conf_file, "=", 0, 1,
					Definition._ENCODING);
			order_dict_ = CommonDataAndFunc.readOrderMap(conf_file, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load column_spec");

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

			union_dict_ = new HashMap<String, List<String>>();
			for (String key : ngram_dict_.keySet()) {
				if (!use_dict_.contains(key))
					continue;
				String[] ts = StringUtils.split(key, "-");
				List<String> value = new ArrayList<String>(ts.length);
				for (int i = 0; i < ts.length; ++i)
					value.add(ts[i]);
				union_dict_.put(key, value);
			}
			System.err.println("success to load make union dict");
		}

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
					System.err.println(value.toString());
					String[] ts = StringUtils.split(value.toString(), "\t");
					if (ts.length != 8) continue;
					int id = Integer.parseInt(ts[7]);
					// drop feature
					if (id < 0) {
						System.err.println("drop id:" + value.toString());
						continue;
					}
					// type, value, encoding_id
					hash_dict_.insert(ts[0], ts[1], Integer.parseInt(ts[7]));
				}
				System.err.println("success to load make feature dict===========" + hash_dict_.getState());
				return;
			}
			
			flag = true;

			// ////// following processes the feature encoding of training
			// samples //////
			// sample weight
			int sum = 0;
			for (Text cnt : values) {
				sum += Integer.parseInt(cnt.toString());
			}

			// header | label | feature
			String[] ts = StringUtils.split(key.toString(), " \\| ");
			if (ts.length < 3) {
				System.err.println("unexpected header: " + key.toString());
				return;
			}

			// wordmap stores original single value, allmap stores union ngram
			// values
			Map<String, String> wordmap = new HashMap<String, String>();
			Map<String, String> allmap = new HashMap<String, String>();

			// parse feature values
			String[] arr = StringUtils.split(ts[2].toString(), CommonDataAndFunc.CTRL_A, -1);
			if (arr.length != order_dict_.size()) return;
			String psid = null;
			for (int i = 0; i < arr.length; ++i) {
				if (arr[i].length() == 0 || arr[i].equals("\\N")) {
					System.err.println("unexpected missing value:"
							+ ts[2].toString());
					return;
				}

				String name = order_dict_.get(i);
				if (name == null) {
					System.err.println("unexpected order " + i + " " + ts[2]);
					return;
				}

				if (name.equals(Definition._PSID)) {
					psid = arr[i];
				}

				if (name.equals(Definition._LABEL)) {
					continue;
				}
				wordmap.put(name, arr[i]);
				if (use_dict_.contains(name)) {
					allmap.put(name, arr[i]);
				}
			} // end for

			// check and filtering
			if (psid == null) {
				return;
			}

			// union feature
			for (String ukey : union_dict_.keySet()) {
				String uv = "";
				boolean flag = true;
				List<String> _values = union_dict_.get(ukey);
				for (String f : _values) {
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

			// value => id
			String result = ts[1] + " " + sum + " |";
			for (String name : allmap.keySet()) {
				Integer id = hash_dict_.getValue(name, allmap.get(name));
				if (id == null) {
					System.out.println("drop id: " + name + "==="
							+ allmap.get(name));
					return;
				}
				result += " " + id;
			}

			context.write(new Text(result), new Text(""));
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(EncodingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(EncodingPartitioner.class);

		job.setCombinerClass(EncodingCombiner.class);

		job.setReducerClass(EncodingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
