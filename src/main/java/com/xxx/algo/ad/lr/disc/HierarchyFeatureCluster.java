package com.xxx.algo.ad.lr.disc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.xxx.algo.ad.lr.disc.utils.*;
import com.xxx.algo.ad.mr.CommonDataAndFunc;

public class HierarchyFeatureCluster implements FeatureCluster {
	private AtomicInteger unique;
	private String name = null;
	private int clkNum;
	
	private HashMap<String, Float> psidCtr;

	public HierarchyFeatureCluster(AtomicInteger _unique, HashMap<String, Float> _psidCtr, int _clkNum) {
		unique = _unique;
		psidCtr = _psidCtr;
		clkNum = _clkNum;
	}

	private int getId() {
		return unique.getAndAdd(1);
	}

	@Override
	public HashMap<String, Feature> CC(HashMap<String, Feature> terms) {
		for (Entry<String, Feature> entry : terms.entrySet()) {
			entry.getKey().replace("_PINPAI_CPC", CommonDataAndFunc.CTRL_A);
		}
		terms = cluster(terms, '#', "", 1);
		for (Entry<String, Feature> entry : terms.entrySet()) {
			entry.getKey().replace(CommonDataAndFunc.CTRL_A, "_PINPAI_CPC");
		}
		return terms;
	}

	private String getParts(String key, char splitChar, int st, int ed) {
		String[] ts = key.split(splitChar + "");
		if (st < 0)
			st = 0;
		if (ed < 0)
			ed = ts.length - 1;
		String ret = "";
		for (int i = st; i <= ed; ++i) {
			ret += ts[i];
			if (i < ed) {
				ret += splitChar;
			}
		}
		return ret;
	}

	private HashMap<String, Feature> cluster(HashMap<String, Feature> terms,
			char splitChar, String parent, long pvNum) {
		if (terms.entrySet().iterator().next().getKey().length() == 0) {
			for (Entry<String, Feature> entry : terms.entrySet()) {
				entry.getValue().id = getId();
			}
			return terms;
		}

		Map<String, Long> pv = new HashMap<String, Long>();
		Map<String, Long> clk = new HashMap<String, Long>();
		List<String> mergeList = new ArrayList<String>();
		List<String> normList = new ArrayList<String>();
		List<String> dropList = new ArrayList<String>();
		Map<String, HashMap<String, Feature>> mapsInside = new HashMap<String, HashMap<String, Feature>>();
		HashMap<String, Feature> res = new HashMap<String, Feature>();

		for (Entry<String, Feature> entry : terms.entrySet()) {
			String part = getParts(entry.getKey(), splitChar, 0, 0);
			long oldPv = 0;
			if (pv.containsKey(part)) {
				oldPv = pv.get(part);
			}
			pv.put(part, oldPv + entry.getValue().pv);
			long oldClk = 0;
			if (clk.containsKey(part)) {
				oldClk = clk.get(part);
			}
			clk.put(part, oldClk + entry.getValue().clk);
			if (name == null) {
				name = entry.getValue().name;
			}
			if (!mapsInside.containsKey(part)) {
				mapsInside.put(part, new HashMap<String, Feature>());
			}
			mapsInside.get(part).put(
					getParts(entry.getKey(), splitChar, 1, -1),
					entry.getValue());
		}

		for (Entry<String, Long> entry : pv.entrySet()) {
			long _clk = clk.get(entry.getKey());
			long _pv = entry.getValue();
			float ctr = _clk * 1f / _pv;
			if (ctr > Consts.MAX_CTR) {
				dropList.add(entry.getKey());
			} else if (_clk < clkNum && _pv < pvNum) {
				mergeList.add(entry.getKey());
			} else {
				normList.add(entry.getKey());
			}
		}

		for (String key : dropList) {
			for (Entry<String, Feature> entry : mapsInside.get(key).entrySet()) {
				entry.getValue().attr = "_drop_";
				entry.getValue().id = -100;
				res.put(key + splitChar + entry.getKey(), entry.getValue());
			}
		}

		if (mergeList.size() > 0) {
			int mergeId = getId();
			for (String key : mergeList) {
				for (Entry<String, Feature> entry : mapsInside.get(key)
						.entrySet()) {
					entry.getValue().attr = "_merge_";
					entry.getValue().id = mergeId;
					res.put(key + splitChar + entry.getKey(), entry.getValue());
				}
			}
		}

		for (String key : normList) {
			// if key is psid, build the sample pv here.
			if (splitChar == '#') {
				Float ctr = psidCtr.get(key);
				if (ctr == null) { // if psid list is empty, use 0.001 as default ctr 
					ctr = 0.001f;
				}
				pvNum = (long)(clkNum / ctr);
			}
			HashMap<String, Feature> tmp = cluster(mapsInside.get(key), '_',
					parent + key + splitChar, pvNum);
			for (Entry<String, Feature> entry : tmp.entrySet()) {
				res.put(key + splitChar + entry.getKey(), entry.getValue());
			}
		}
		return res;
	}

}
