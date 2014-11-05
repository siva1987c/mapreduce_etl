package com.xxx.algo.ad.lr.disc.utils;

public class FeatureHash {
	public static int GetCodeInRange(String feature, int type, int range) {
		int mask = 1;
		while (mask <= range) mask <<= 1;
		--mask;
		long code = GetStringHash(feature, type);
		return (int) getUnsignedInt(type + getUnsignedInt(getUnsignedInt(code & mask) % range));
	}
	// per-kind feature own its specific hash space
	public static int GetCode(String feature, int type, int maskbits) {
		int mask = (1<<maskbits) - 1;
		long code = GetStringHash(feature, type);
		return (int) getUnsignedInt(type + getUnsignedInt(code & mask));
	}
    // all kind features own a shared hash space
	public static long GetCode(String feature, String type, int maskbits) {
		int mask = (1<<maskbits) - 1;
		long random_seed=getUnsignedInt(GetStringHash(type,0));
		return (long) (getUnsignedInt(GetStringHash(feature,random_seed)) & mask);
	}
	
	private static long getUnsignedInt(long data) {
		return data & (((long)1<<32)-1);
	}
	
	private static long GetStringHash(String feature, long seed) {
		if (feature == null) 
			return seed;
		long ret = 0;
		feature.trim();
		for (int i = 0; i < feature.length(); ++i) {
			char c = feature.charAt(i);
			if (c >= '0' && c <= '9') {
				ret = getUnsignedInt(10 * ret + c - '0');
			} else {
				return UniformHash(feature, seed);
			}
		}
		return getUnsignedInt(ret + seed);
	}
	
	private static long UniformHash(String str, long seed) {
		long c1 = 0xcc9e2d51;
		long c2 = 0x1b873593;
		
		long h1 = seed;
		byte[] data = str.getBytes();
		int nblocks = str.length() / 4;
		
		// --- body
		for (int i = 0; i < nblocks; ++i) {
			long k1 = byteArrayToInt(data, i * 4);
			k1 = getUnsignedInt(k1 * c1);
			k1 = ROTL32(k1,15);
			k1 = getUnsignedInt(k1 * c2);

			h1 = getUnsignedInt(h1 ^ k1);
			h1 = ROTL32(h1,13);
			h1 = getUnsignedInt(h1*5 + (long)0xe6546b64);
		}
		
		// --- tail
		int offset = nblocks * 4;

		long k1 = 0;
		switch(str.length() & 3) {
		case 3: k1 ^= byteToInt(data[offset + 2]) << 16;
		case 2: k1 ^= byteToInt(data[offset + 1]) << 8;
		case 1: k1 ^= byteToInt(data[offset]);
			k1 = getUnsignedInt(k1 * c1);
			k1 = ROTL32(k1,15); 
			k1 = getUnsignedInt(k1 * c2);
			h1 = getUnsignedInt(h1 ^ k1);
		}

		// --- finalization
		h1 = getUnsignedInt(h1 ^ str.length());
		
		return FMIX(h1);
	}
	
	private static long ROTL32 (long x, long r) {
		return getUnsignedInt((x << r) | (x >> (32 - r)));
	}
	
	private static long FMIX (long h) {
		h = getUnsignedInt(h ^ h >> 16);
		h = getUnsignedInt(h * 0x85ebca6b);
		h = getUnsignedInt(h ^ h >> 13);
		h = getUnsignedInt(h * 0xc2b2ae35);
		h = getUnsignedInt(h ^ h >> 16);
		return h;
	}
	
	private static long byteToInt(byte b) {
		return (b & 0x000000FF);
	}
	
	private static long byteArrayToInt(byte[] b, int offset) {
		long value = 0;
	    for (int i = 0; i < 4; i++) {
	    	int shift= i * 8;
	        value += ((b[i + offset] & 0x000000FF) << shift); 
	    }
	    return value;
	}
	
	 public static void main(String[] args) throws Exception {
		 long code = GetCode("PDPS000000033239", "psidid", 18);
		 System.out.println("code:" + code);
	 }
}
