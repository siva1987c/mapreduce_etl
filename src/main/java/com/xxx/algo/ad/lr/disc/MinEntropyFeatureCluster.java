package com.xxx.algo.ad.lr.disc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.xxx.algo.ad.lr.disc.utils.*;

public class MinEntropyFeatureCluster implements FeatureCluster {

	private String name = "";
	String prefix = "";
	private long MIN_PV = 1;
	private double shrink_scale = 1.0;
	private AtomicInteger unique;

	public MinEntropyFeatureCluster(AtomicInteger _unique, String _prefix,
			double _shrink_scale) {
		unique = _unique;
		name = "";
		MIN_PV = 1;
		prefix = _prefix;
		shrink_scale = _shrink_scale;
	}

	private double listEntropy(ArrayList<Element> elist) {
		double sum = 0;
		for (int i = 0; i < elist.size(); ++i) {
			sum += elist.get(i).pv * elist.get(i).entropy();
		}
		return sum;
	}

	private ArrayList<Element> iterOptimize(ArrayList<Element> elist,
			double minEntropy) {
		ArrayList<Element> result = new ArrayList<Element>();
		IterList srclist = new IterList(elist);
		int oldSize = srclist.cluster.size();

		int idx = 0;
		while (true) {
			++idx;
			IterList tmplist = srclist.divide(MIN_PV);
			result = tmplist.toElementList();
			double newEnt = listEntropy(result);
			int newSize = tmplist.cluster.size();
			double psMse = (newSize - oldSize) / (newSize + 0.000001);
			double peMse = (newEnt - minEntropy) / (newEnt + 0.000001);
			if (psMse < 0.005 || peMse < 0.0005 || idx > 50) {
				break;
			}
			srclist = tmplist;
			oldSize = newSize;
		}
		ArrayList<Element> reverseResult = new ArrayList<Element>();
		for (int i = result.size() - 1; i >= 0; --i) {
			reverseResult.add(result.get(i));
		}
		return reverseResult;
	}

	@Override
	public HashMap<String, Feature> CC(HashMap<String, Feature> terms) {
		double tpv = 0.0001;
		double tclk = 0;
		ArrayList<Element> elist = new ArrayList<Element>();
		for (Entry<String, Feature> entry : terms.entrySet()) {
			Feature f = entry.getValue();
			name = f.name;
			Element e = new Element();
			e.set(Double.parseDouble(f.value), f.pv, f.clk);
			elist.add(e);
			tpv += f.pv;
			tclk += f.clk;
		}
		MIN_PV = (long) (shrink_scale * tpv / Math.max(1.0,
				Math.sqrt(tclk + 1.0)));
		Collections.sort(elist);
		if (elist.get(0).value < 1e-10 && elist.size() > 1) {
			elist.get(0).value = elist.get(1).value / 2;
		}
		double rawEntropy = listEntropy(elist);
		ArrayList<Element> result = iterOptimize(elist, rawEntropy);
		HashMap<String, Feature> model = new HashMap<String, Feature>();
		for (int i = 0; i < result.size(); ++i) {
			Element e = result.get(i);
			Feature nf = new Feature();
			nf.name = name;
			nf.value = String.format("%.6f", e.value);
			nf.pv = (long) e.pv;
			nf.clk = (long) e.clk;
			if (model.containsKey(prefix + "#" + nf.value)) {
				Feature f = model.get(prefix + "#" + nf.value);
				f.pv += nf.pv;
				f.clk += nf.clk;
				if (i == result.size() - 1) {
					f.value = "1.000000";
				}
				model.put(prefix + "#" + nf.value, f);
			} else {
				nf.id = unique.getAndAdd(1);
				if (i == result.size() - 1) {
					nf.value = "1.000000";
				}
				model.put(prefix + "#" + nf.value, nf);
			}
		}
		return model;
	}
}
