package com.xxx.algo.ad.lr.disc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.xxx.algo.ad.lr.disc.utils.*;

public class HashFeatureCluster implements FeatureCluster {
	private AtomicInteger unique;

	public HashFeatureCluster(AtomicInteger _unique) {
		unique = _unique;
	}

	private boolean legal(HashSet<Feature> hs, long limit) {
		long totalPv = 0, sumPv = 0;
		for (Feature f : hs) {
			if (f.pv >= limit)
				sumPv += f.pv;
			totalPv += f.pv;
		}
		return totalPv * 0.9 <= sumPv;
	}

	private int count(HashSet<Feature> hs, long limit) {
		int cnt = 0;
		for (Feature f : hs) {
			if (f.pv >= limit)
				++cnt;
		}
		return cnt;
	}

	private int getHashSize(HashSet<Feature> hs) {
		long left = 0, right = Long.MAX_VALUE / 2L, mid;
		while (left < right) {
			if (left + 1L == right) {
				if (legal(hs, right))
					left = right;
				break;
			}
			mid = (left + right + 1L) >> 1L;
			if (legal(hs, mid)) {
				left = mid;
			} else {
				right = mid - 1L;
			}
		}
		return count(hs, left);
	}

	@Override
	public HashMap<String, Feature> CC(HashMap<String, Feature> terms) {
		HashMap<String, HashSet<Feature>> psidMap = new HashMap<String, HashSet<Feature>>();
		for (Entry<String, Feature> entry : terms.entrySet()) {
			HashSet<Feature> hsInside = new HashSet<Feature>();
			String psid = entry.getKey().split("#")[0];
			if (psidMap.containsKey(psid)) {
				hsInside = psidMap.get(psid);
			}
			hsInside.add(entry.getValue());
			psidMap.put(psid, hsInside);
		}
		for (Entry<String, HashSet<Feature>> entry : psidMap.entrySet()) {

			HashSet<Feature> hs = entry.getValue();
			int size = getHashSize(hs), offset = unique.get();
			System.err.println(entry.getKey() + "," + size + "," + offset);
			for (Feature f : hs) {
				f.id = FeatureHash.GetCodeInRange(f.value, offset, size);
			}
			unique.addAndGet(size);
		}
		return terms;
	}
}
