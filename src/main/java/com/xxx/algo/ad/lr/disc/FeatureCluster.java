package com.xxx.algo.ad.lr.disc;

import java.util.HashMap;

import com.xxx.algo.ad.lr.disc.utils.Feature;

public abstract interface FeatureCluster {
	public HashMap<String, Feature> CC(HashMap<String, Feature> terms);
}
