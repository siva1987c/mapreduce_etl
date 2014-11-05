package com.xxx.algo.ad.lr.disc.utils;

public class Element implements Comparable<Element> {
	public double value = 0.;
	public long pv = 0;
	public long clk = 0;

	public Element() {
		value = 0.;
		pv = 0;
		clk = 0;
	}

	public void set(double v, long _pv, long _clk) {
		value = v;
		pv = _pv;
		clk = _clk;
	}

	public static double entropy(long pv, long clk) {
		if (pv <= 0 || clk <= 0 || clk == pv)
			return 0;
		double pc = clk * 1. / pv;
		double pp = 1. - pc;
		double e = pc * Math.log(pc) + pp * Math.log(pp);
		return -e;
	}

	public double entropy() {
		return entropy(pv, clk);
	}

	@Override
	public int compareTo(Element o) {
		return new Double(value).compareTo(new Double(o.value));
	}
}
