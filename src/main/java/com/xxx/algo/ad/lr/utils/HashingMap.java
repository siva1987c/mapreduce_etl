package com.xxx.algo.ad.lr.utils;

import java.util.HashMap;

public class HashingMap<T> {
	private HashMap<Long, T> dict;
	private long totalCnt = 0, conflictCnt = 0;

	public HashingMap() {
		dict = new HashMap<Long, T>();
	}

	public void insert(String name, String value, T id) {
		long hash = getHash(name + "^" + value);
		//System.err.println(name + "," + value + "," + id + "------" + hash);
		if (dict.containsKey(hash))
			++conflictCnt;
		++totalCnt;
		dict.put(hash, id);
	}

	public void insert(String value, T id) {
		long hash = getHash(value);
		if (dict.containsKey(hash))
			++conflictCnt;
		++totalCnt;
		dict.put(hash, id);
	}

	public void insertHash(long hash, T id) {
		if (dict.containsKey(hash)) 
			++conflictCnt;
		++totalCnt;
		dict.put(hash, id);
	}

	public boolean containsKey(String value) {
		return dict.containsKey(getHash(value));
	}

	public boolean containsKey(String name, String value) {
		return dict.containsKey(getHash(name + "^" + value));
	}

	public String getState() {
		return "totalCnt:\t" + totalCnt + "\tconflictCnt:\t" + conflictCnt;
	}

	public T getValue(String name, String value) {
		//System.err.println(name + "," + value + "------" + getHash(name + "^" + value));
		return dict.get(getHash(name + "^" + value));
	}

	private static long getUnsignedInt(long data) {
		return data & (((long) 1 << 32) - 1);
	}

	private long BKDRHash(String str) {
		long seed = 13131;
		long hash = 0;
		for (int i = 0, len = str.length(); i < len; ++i) {
			hash = getUnsignedInt(hash * seed + str.charAt(i));
		}
		return (hash & 0x7FFFFFFF);
	}
	
	private int iabs(int x) {
		return (x > 0) ? x : -x;
	}

	private long DEFAULTHash(String str) {
		return iabs(str.hashCode());
	}

	private long getHash(String str) {
		return (DEFAULTHash(str) << 32) + BKDRHash(str);
	}
}
