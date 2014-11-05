package com.xxx.algo.ad.lr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import com.xxx.algo.ad.lr.utils.*;
import com.xxx.algo.ad.mr.*;

public class Evaluation extends AbstractProcessor {

	public static class EvaluationMapper extends
			Mapper<Object, Text, Text, Text> {

		Map<String, String> type_dict_; // feature => type
		Map<Integer, String> order_dict_; // line no.=> feature

		Set<String> use_dict_; // feature set after filtering
		Map<String, String> ngram_dict_; // feature union => union type
		Map<String, List<String>> union_dict_; // feature union => how to union

		Map<String, String> psids_; // psid => reducer id

		HashingMap<Float> model_; // featurehash => weight

		Map<String, Float> ps_rate_ = new HashMap<String, Float>(); // pdps
																	// sample
																	// rate

		Map<String, HashMap<String, Long>> defaultCntMap;

		public void setup(Context context) {
			String conf_file = context.getConfiguration().get("column_spec",
					"column.spec");
			type_dict_ = CommonDataAndFunc.readMaps(conf_file, "=", 0, 1,
					Definition._ENCODING);
			order_dict_ = CommonDataAndFunc.readOrderMap(conf_file, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load column_spec");

			//////////////////////////////////////////////////////////////////////
			// reading use.spec
			String use_conf = context.getConfiguration().get("use_spec",
					"use.spec");
			use_dict_ = CommonDataAndFunc.readSets(use_conf, "=", 0,
					Definition._ENCODING);
			System.err.println("success to load use_spec");

			// reading ngram.spec
			String ngram_conf = context.getConfiguration().get("ngram_spec",
					"ngram.spec");
			ngram_dict_ = CommonDataAndFunc.readMaps(ngram_conf, "=", 1, 0,
					Definition._ENCODING);
			System.err.println("success to load ngram_spec");

			// reading union dict
			union_dict_ = new HashMap<String, List<String>>();
			for (String key : ngram_dict_.keySet()) {
				if (!use_dict_.contains(key))
					continue;
				String[] ts = key.split("-");
				List<String> value = new ArrayList<String>(ts.length);
				for (int i = 0; i < ts.length; ++i)
					value.add(ts[i]);
				union_dict_.put(key, value);
			}
			System.err.println("success to load make union dict");

			// ////////////////////////////////////////////////////////////////////////////
			// reading psid.spec
			String psid_conf = context.getConfiguration().get("psid_spec",
					"psid.spec");
			psids_ = CommonDataAndFunc.readMaps(psid_conf, " ", 0, 1,
					Definition._ENCODING);
			System.err.println("success to load make psid spec");

			// //////////////////////////////////////////////////////////////
			// reading model.spec
			String model_conf = context.getConfiguration().get("model_spec",
					"model.spec");
			model_ = readModel(model_conf);
			System.err.println("success to load model");
			
			// reading sampling pv and calculate sample rate for each pdps
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

			// init default counter, in order to calculate default rates
			defaultCntMap = new HashMap<String, HashMap<String, Long>>();
		}

		// reading model.spec file
		public HashingMap<Float> readModel(String file_name) {
			HashingMap<Float> model = new HashingMap<Float>();
			try {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new FileInputStream(file_name)));
				String line = "";
				while ((line = reader.readLine()) != null) {
					if (line.startsWith("#"))
						continue;
					String[] terms = StringUtils.split(line.trim(), ":", -1);
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
			System.out.println("STATE:" + model.getState());
			return model;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String label = null;
			String psid = "";
			Float sum_w = 0.f;

			Map<String, String> wordmap = new HashMap<String, String>();
			Map<String, String> allmap = new HashMap<String, String>();

			Map<String, Integer> cnt = new HashMap<String, Integer>();

			String[] arr = StringUtils.split(value.toString(), CommonDataAndFunc.CTRL_A, -1);
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
			if (!psids_.containsKey(psid))
				return;

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
					int len = uv.length();
					uv = uv.substring(0, len - 1);
					allmap.put(ukey, uv);
				}
			}

			// feature value => weight
			for (String name : allmap.keySet()) {
				String fv = allmap.get(name);

				int defaultCnt = 0;

				if (!model_.containsKey(name, fv)) {
					List<String> values = union_dict_.get(name);
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
							defaultCnt = values.size() - end;
							break;
						} else {
							end -= 1;
						}
					} // end while
				} // end if
				
				if (defaultCnt > 0) {
					System.err.println("default\t==========\t" + fv);
				}

				int base = 0;
				if (cnt.containsKey(defaultCnt)) {
					base = cnt.get(defaultCnt);
				}
				++base;
				cnt.put(name + "^" + defaultCnt, base);
				Float w = model_.getValue(name, fv);
				if (w == null) {
					System.err.println("Not found weight:" + name + "^" + fv);
				} else {
					sum_w += w;
				}
			}

			// for reverse sort
			double prediction = 1.0 - 1.0 / (1 + Math.exp(-sum_w
					- Math.log(ps_rate_.get(psid))));

			String region = psids_.get(psid);

			if (!defaultCntMap.containsKey(region)) {
				defaultCntMap.put(region, new HashMap<String, Long>());
			}
			HashMap<String, Long> mapInside = defaultCntMap.get(region);
			for (Entry<String, Integer> entry : cnt.entrySet()) {
				Long base = mapInside.get(entry.getKey());
				if (base == null)
					base = new Long(0);
				++base;
				mapInside.put(entry.getKey(), base);
			}

			// output
			String header = region + "@" + String.format("%.6f", prediction);
			if (label.equals("-1")) {
				context.write(new Text(header), new Text("0\t1"));
			} else {
				context.write(new Text(header), new Text("1\t1"));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Entry<String, HashMap<String, Long>> entry : defaultCntMap
					.entrySet()) {
				for (Entry<String, Long> entryInside : entry.getValue()
						.entrySet()) {
					context.write(
							new Text(entry.getKey() + "@#"),
							new Text(entryInside.getKey() + "\t"
									+ entryInside.getValue()));
				}
			}
		}
	}

	public static class EvaluationPartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int nr) {
			String[] ts = key.toString().split("@");
			String part = ts[0];
			return Integer.parseInt(part) % nr;
		}

	}

	public static class EvaluationCombiner extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (key.toString().endsWith("#")) {
				for (Text value : values) {
					context.write(key, value);
				}
				return;
			}

			int pv = 0;
			int click = 0;
			for (Text text : values) {
				String[] terms = StringUtils.split(text.toString(), "\t", -1);
				if (terms.length < 2) {
					continue;
				}
				int click_cur = Integer.parseInt(terms[0]);
				int pv_cur = Integer.parseInt(terms[1]);
				click += click_cur;
				pv += pv_cur;
			}
			context.write(key, new Text(click + "\t" + pv));
		}
	}

	public static class EvaluationReducer extends
			Reducer<Text, Text, Text, Text> {
		class Point implements Comparable<Point> {
			public Float x;
			public Float y;

			public Point(Float _x, Float _y) {
				x = _x;
				y = _y;
			}

			@Override
			public int compareTo(Point o) {
				return x.compareTo(o.x);
			}
		}

		private long click_sum = 0;
		private long noclick_sum = 0;
		private double auc = 0;
		private String psid = null;
		private double sumMSE = 0;
		private long totalPv = 0;
		Map<String, String> psids_; // psid => reducer id
		List<Long> noclks;
		List<Long> clks;
		List<Point> pts;
		private HashMap<String, Long> cnt = new HashMap<String, Long>();

		@Override
		public void setup(Context context) {
			click_sum = 0;
			noclick_sum = 0;
			auc = 0;
			psid = "";
			noclks = new ArrayList<Long>();
			clks = new ArrayList<Long>();
			pts = new ArrayList<Point>();
			String psid_conf = context.getConfiguration().get("psid_spec",
					"psid.spec");
			psids_ = CommonDataAndFunc.readMaps(psid_conf, " ", 1, 0,
					Definition._ENCODING);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (key.toString().endsWith("#")) {
				for (Text value : values) {
					String[] ts = StringUtils.split(value.toString(), "\t", -1);
					long v = Long.parseLong(ts[1]);
					long base = 0;
					if (cnt.containsKey(ts[0])) {
						base = cnt.get(ts[0]);
					}
					base += v;
					cnt.put(ts[0], base);
				}
				return;
			}
			String header = key.toString();
			String[] ts = header.split("@");
			if (ts.length < 2) {
				System.err.println("Unexpected reducer key:" + header);
				return;
			}
			psid = ts[0];
			double ctr = 1.0 - Double.parseDouble(ts[1]);

			long pv = 0;
			long click = 0;
			double no_click = 0.0;
			for (Text value : values) {
				String[] terms = StringUtils.split(value.toString(), "\t", -1);
				if (terms.length < 2)
					continue;
				long now_click = Long.parseLong(terms[0]);
				long now_pv = Long.parseLong(terms[1]);
				click += now_click;
				pv += now_pv;
				no_click += now_pv - now_click;
			}
			long old_click_sum = click_sum;
			click_sum += click;
			noclick_sum += no_click;
			totalPv += pv;

			double realCtr = click * 1. / pv;
			sumMSE += pv * (ctr - realCtr) * (ctr - realCtr);

			auc += (old_click_sum + click_sum) * no_click / 2.0;

			context.write(new Text(String.format("%.6f", ctr)), new Text(pv
					+ "\t" + click));
			noclks.add(pv - click);
			clks.add(click);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			long noclk_sum = 0, clk_sum = 0;
			for (int i = 0; i < noclks.size(); ++i) {
				long noclk = noclks.get(i), clk = clks.get(i);
				long tp = click_sum - clk_sum, fp = noclick_sum - noclk_sum, fn = clk_sum;
				noclk_sum += noclk;
				clk_sum += clk;
				pts.add(new Point(tp * 1f / (tp + fn), tp * 1f / (tp + fp)));
			}
			Collections.sort(pts);
			float areaSum = 0;
			for (int i = 1; i < pts.size(); ++i) {
				areaSum += (pts.get(i).x - pts.get(i - 1).x)
						* (pts.get(i).y + pts.get(i - 1).y) * 0.5f;
			}
			auc = auc / click_sum / noclick_sum;
			String header = psids_.get(psid) + ":"
					+ String.format("%.10f", auc) + ","
					+ String.format("%.10f", sumMSE / totalPv) + ","
					+ String.format("%.10f", areaSum);
			context.write(new Text(header), new Text(""));
			for (Entry<String, Long> entry : cnt.entrySet()) {
				context.write(new Text(psids_.get(psid) + "\t" + entry.getKey()
						+ "\t" + entry.getValue()), new Text(""));
			}
		}
	}

	@Override
	protected void configJob(Job job) {
		job.setMapperClass(EvaluationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(EvaluationPartitioner.class);

		job.setCombinerClass(EvaluationCombiner.class);

		job.setReducerClass(EvaluationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
