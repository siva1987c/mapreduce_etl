package com.xxx.algo.ad.lr.disc.utils;

public class Name implements Comparable<Name> {
	public String type = null;
	public String name = null;

	@Override
	public int compareTo(Name arg0) {
		if (!type.equals(arg0.type)) {
			return type.compareTo(arg0.type);
		}
		return name.compareTo(arg0.name);
	}

	public Name(String[] names) {
		type = names[0];
		if (names.length == 2) {
			name = names[1];
		}
	}
}
