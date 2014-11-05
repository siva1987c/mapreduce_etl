package com.xxx.algo.ad.mr;

/**
 * A simple pair
 * 
 */
public class MyPair<A, B> {

	private A fst;
	private B snd;

	public MyPair(A fst, B snd) {
		super();
		this.fst = fst;
		this.snd = snd;
	}

	public A getFirst() {
		return fst;
	}

	public B getSecond() {
		return snd;
	}

	public void setFirst(A v) {
		fst = v;
	}

	public void setSecond(B v) {
		snd = v;
	}

	public String toString() {
		return "MyPair[" + fst + "," + snd + "]";
	}

	private static boolean equals(Object x, Object y) {
		return (x == null && y == null) || (x != null && x.equals(y));
	}

	@SuppressWarnings("rawtypes")
	public boolean equals(Object other) {
		return other instanceof MyPair && equals(fst, ((MyPair) other).fst)
				&& equals(snd, ((MyPair) other).snd);
	}

	public int hashCode() {
		if (fst == null)
			return (snd == null) ? 0 : snd.hashCode() + 1;
		else if (snd == null)
			return fst.hashCode() + 2;
		else
			return fst.hashCode() * 17 + snd.hashCode();
	}

	public static <A, B> MyPair<A, B> of(A a, B b) {
		return new MyPair<A, B>(a, b);
	}
}
