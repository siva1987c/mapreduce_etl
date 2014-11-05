package com.xxx.algo.ad.lr.utils;

public class Definition {
	public static final String _LABEL = "label";
	public static final String _PSID = "ps";
	public static final String _LIPV = "pv";
	public static final int _MAX_LIPV = 100;
	public static final String _LICTR = "pli";
	public static String PCTR = "pctr";

	public static final String _ENCODING = "utf-8";
	
	public static String PSLI = "ps-li";
	public static boolean isPctrType(String type) {
		return type.equals("ps-ptr") || type.equals("ps-pcu") || type.equals("ps-pgr") || type.equals("ps-pli");
	}
	public static boolean isCtrType(String type) {
		return type.equals("ptr") || type.equals("pcu") || type.equals("pgr") || type.equals("pli");
	}
}
