package com.xxx.algo.ad.lr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.xxx.algo.ad.lr.utils.Definition;
import com.xxx.algo.ad.mr.*;

public class FeatureSpec extends AbstractProcessor {

	public static class FeatureMapper extends Mapper<LongWritable, Text, Text, Text> {

		Map<String, String> type_dict_; // feature => type
		Map<Integer, String> order_dict_; // line no.=> feature

		Map<String, String> ngram_dict_; // feature union => union type
		Map<String, List<String>> union_dict_; // feature union => how to union

		public void setup(Context context) {
			String conf_file = context.getConfiguration().get("column_spec",
					"column.spec");
			type_dict_ = CommonDataAndFunc.readMaps(conf_file, "=", 0, 1,
					Definition._ENCODING);
			order_dict_ = CommonDataAndFunc.readOrderMap(conf_file, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load column_spec");

			String ngram_conf = context.getConfiguration().get("ngram_spec",
					"ngram.spec");
			ngram_dict_ = CommonDataAndFunc.readMaps(ngram_conf, "=", 1, 0,
					Definition._ENCODING);
			System.err.println("success to load ngram_spec");

			union_dict_ = new HashMap<String, List<String>>();
			for (String key : ngram_dict_.keySet()) {
				String[] ts = StringUtils.split(key, "-");
				List<String> value = new ArrayList<String>(ts.length);
				for (int i = 0; i < ts.length; ++i)
					value.add(ts[i]);
				union_dict_.put(key, value);
			}
			System.err.println("success to load make union dict");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			LinkedList<MyPair<String, String>> result = new LinkedList<MyPair<String, String>>();
			Map<String, String> wordmap = new HashMap<String, String>();

			String pv_clk = null;
			String[] arr = StringUtils.split(value.toString(), CommonDataAndFunc.CTRL_A);
			if (arr.length != order_dict_.size()) {
				System.err.println("LOG LENGTH ERROR");
				return;
			}
			for (int i = 0; i < arr.length; ++i) {
				String name = order_dict_.get(i);
				if (name == null) {
					System.err.println("unexpected order " + i + " " + value);
					continue;
				}
				
				if (name.equals(Definition._LABEL)) {
					if (arr[i].equals("-1")) {
						pv_clk = "1\t0";
					} else {
						pv_clk = "1\t1";
					}
				} else {
					// enum feature
					if (arr[i].equals("\\N")) {
						System.err.println("found empty feature " + name + " "
								+ value);
					}
					
					if (name.equals(Definition._PSID)) {
						result.add(new MyPair<String, String>(name, arr[i]));
					}
					
					if (name.equals(Definition._LIPV)) {
						if (Integer.parseInt(arr[i]) > Definition._MAX_LIPV) {
							arr[i] = Definition._MAX_LIPV + "";
						}
					}
					
					wordmap.put(name, arr[i]);
				}
			}

			if (pv_clk == null) {
				System.err.println("unexpected label [" + value.toString()
						+ "]");
				return;
			}

			// TODO: whether to filter empty feature sample

			// union feature
			for (String ukey : union_dict_.keySet()) {
				String uv = "";
				boolean flag = true;
				List<String> values = union_dict_.get(ukey);
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
					uv = uv.substring(0, uv.length() - 1);
					result.add(new MyPair<String, String>(ukey, uv));
				}
			}

			// output count
			for (MyPair<String, String> item : result) {
				String okey = item.getFirst() + "^" + item.getSecond();
				context.write(new Text(okey), new Text(pv_clk));
			}
		}

	}

	public static class FeatureReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long total_pv = 0;
			long total_clk = 0;
			for (Text value : values) {
				String[] arr = StringUtils.split(value.toString(), "\t");
				if (arr.length < 2) {
					System.err.println("unexpected mapper output: "
							+ value.toString());
					continue;
				}
				long pv = Long.parseLong(arr[0]);
				long clk = Long.parseLong(arr[1]);
				total_pv += pv;
				total_clk += clk;
			}
			String result = String.format("%d\t%d", total_pv, total_clk);
			context.write(key, new Text(result));
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(FeatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(FeatureReducer.class);

		job.setReducerClass(FeatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
