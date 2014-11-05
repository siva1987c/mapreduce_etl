package com.xxx.algo.ad.lr.disc.utils;

public class Feature implements Comparable<Feature> {
	public String name = "";
	public String value = "";
	public String attr = "_norm_";
	public long pv = 0;
	public long clk = 0;
	public int id = -1;
	public float weight;

	public Feature() {
	}

	public Feature(String _name, String _value, long _pv, int _id, float _weight) {
		name = _name;
		value = _value;
		pv = _pv;
		id = _id;
		weight = _weight;
	}

	public void set(String _name, String _value, long _pv, long _clk) {
		name = _name;
		value = _value;
		pv = _pv;
		clk = _clk;
	}

	public void setByLine(String[] ts) {
		if (ts.length < 8)
			return;
		name = ts[0];
		value = ts[1];
		attr = ts[2];
		pv = Long.parseLong(ts[3]);
		clk = Long.parseLong(ts[4]);
		weight = Float.parseFloat(ts[6]);
		id = Integer.parseInt(ts[7]);
	}

	public void set(String _name, String _value, int _id) {
		name = _name;
		value = _value;
		id = _id;
	}

	public void set(String _name, String _value, float _weight) {
		name = _name;
		value = _value;
		weight = _weight;
	}

	public void update(long _pv, long _clk) {
		pv += _pv;
		clk += _clk;
	}

	@Override
	public int compareTo(Feature o) {
		if (id < o.id)
			return -1;
		else if (id > o.id)
			return 1;
		else
			return value.compareTo(o.value);
	}

}
