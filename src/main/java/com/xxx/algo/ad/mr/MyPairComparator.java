package com.xxx.algo.ad.mr;

import java.util.Comparator;

/**
 * MyPair Comparator: perform pair sorting due to configured field and type
 * 
 */
public class MyPairComparator<A extends Comparable<A>, B extends Comparable<B>>
		implements Comparator<MyPair<A, B>> {

	/**
	 * sort by which field: first or second
	 */
	public static enum sort_field {
		first, second
	};

	/**
	 * sort in which direction: normal or reverse
	 */
	public static enum sort_type {
		normal, reverse
	};

	/**
	 * @param field
	 *            sort by which field, first or second
	 * @param type
	 *            sort in which direction, normal or reverse
	 */
	public MyPairComparator(sort_field field, sort_type type) {
		m_field = field;
		m_type = type;
	}

	public int compare(MyPair<A, B> p1, MyPair<A, B> p2) {
		// TODO Auto-generated method stub
		if (m_field == sort_field.first) {
			if (m_type == sort_type.normal) {
				return p1.getFirst().compareTo(p2.getFirst());
			} else {
				return p2.getFirst().compareTo(p1.getFirst());
			}
		} else {
			if (m_type == sort_type.normal) {
				return p1.getSecond().compareTo(p2.getSecond());
			} else {
				return p2.getSecond().compareTo(p1.getSecond());
			}
		}
	}

	private sort_field m_field = sort_field.first;
	private sort_type m_type = sort_type.normal;
}
