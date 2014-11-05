package com.xxx.algo.ad.lr.disc.utils;

import java.util.ArrayList;

public class IterList {
	public ArrayList<ArrayList<Element>> cluster;
	private ArrayList<Integer> markers;

	public IterList() {
		cluster = new ArrayList<ArrayList<Element>>();
		markers = new ArrayList<Integer>();
	}

	public IterList(ArrayList<Element> elist) {
		cluster = new ArrayList<ArrayList<Element>>();
		markers = new ArrayList<Integer>();
		cluster.add(elist);
		markers.add(1);
	}

	public double entropy(long pv, long clk, long total_pv, long total_clk) {
		double ent = Element.entropy(pv, clk) * pv;
		ent += Element.entropy(total_pv - pv, total_clk - clk)
				* (total_pv - pv);
		return ent / total_pv;
	}

	public boolean enough(long pv, long total_pv, long MIN_PV) {
		return (pv > MIN_PV && total_pv - pv > MIN_PV);
	}

	public IterList divide(long MIN_PV) {
		ArrayList<ArrayList<Element>> new_cluster = new ArrayList<ArrayList<Element>>();
		ArrayList<Integer> new_markers = new ArrayList<Integer>();
		for (int pos = 0; pos < cluster.size(); ++pos) {
			ArrayList<Element> el = cluster.get(pos);
			int nel = el.size();
			if (markers.get(pos) < 0 || nel < 2) {
				new_markers.add(-1);
				new_cluster.add(el);
				continue;
			}
			long pv = 0, clk = 0, total_pv = 0, total_clk = 0;
			for (int i = 0; i < nel; ++i) {
				total_pv += el.get(i).pv;
				total_clk += el.get(i).clk;
			}
			int margin = -1, step = (nel > 10) ? (int) Math.log(nel) : 1;
			double local_min = Element.entropy(total_pv, total_clk);
			for (int idx = 0; idx < nel; ++idx) {
				Element e = el.get(idx);
				pv += e.pv;
				clk += e.clk;
				if (step > 1 && idx % step != step - 1) {
					continue;
				}
				double local_ent = entropy(pv, clk, total_pv, total_clk);
				boolean good = enough(pv, total_pv, MIN_PV);
				if (local_ent < local_min && good) {
					margin = idx;
					local_min = local_ent;
				}
			}
			if (margin < 0) {
				new_cluster.add(el);
				new_markers.add(-1);
			} else {
				ArrayList<Element> e1 = new ArrayList<Element>();
				for (int i = 0; i <= margin; ++i) {
					e1.add(el.get(i));
				}
				new_cluster.add(e1);

				ArrayList<Element> e2 = new ArrayList<Element>();
				for (int i = margin + 1; i < nel; ++i) {
					e2.add(el.get(i));
				}
				new_cluster.add(e2);

				new_markers.add(1);
				new_markers.add(1);
			}
		}
		IterList result = new IterList();
		result.cluster = new_cluster;
		result.markers = new_markers;
		return result;
	}

	public ArrayList<Element> toElementList() {
		ArrayList<Element> result = new ArrayList<Element>();
		Element next = new Element();
		next.value = -100;
		for (int i = cluster.size() - 1; i >= 0; --i) {
			ArrayList<Element> el = cluster.get(i);
			Element ele = new Element();
			for (int j = 0; j < el.size(); ++j) {
				ele.pv += el.get(j).pv;
				ele.clk += el.get(j).clk;
			}
			if (next.value < 0) {
				ele.value = el.get(el.size() - 1).value;
			} else {
				ele.value = (el.get(el.size() - 1).value + next.value) / 2;
			}
			next = el.get(0);
			result.add(ele);
		}
		return result;
	}
}
