package com.cloudera.vms.utils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HanmingCode {

	private static char[] codes;
	private static Map<String, Short> charMap = new HashMap<String, Short>();

	static {

		try {
			codes = buildCharset();

			for (int i = 0; i < codes.length; i++) {
				charMap.put(codes[i] + "", (short) i);
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(codes.length);
		// for(char c:codes){
		// System.out.println(c);
		// }

	}

	public static byte[] unicode2Bytes(int i) {
		byte[] targets = new byte[2];
		targets[1] = (byte) (i & 0xFF);
		targets[0] = (byte) (i >> 8 & 0xFF);

		return targets;
	}

	public static char[] buildCharset() throws UnsupportedEncodingException {
		int size = 1 << 14;

		char[] codes = new char[size];
		String start = "\\u4e00";
		String end = "\\u9fa5";
		int s = Integer.parseInt(start.substring(2, start.length()), 16);
		int e = Integer.parseInt(end.substring(2, end.length()), 16);
		int k = 0;
		for (int i = s; i <= e; i++) {
			if (k == size) {
				break;
			}
			codes[k] = (char) i;
			k++;
		}
		return codes;

	}

	public static String encode(byte[] bytes) {
		int currentBitNum = 0;
		int number = 0;
		int radix = 14;
		StringBuilder s = new StringBuilder();
		for (byte b : bytes) {
			for (int i = 0; i < 8; i++) {
				currentBitNum++;
				int t = b << i;
				t = t & 0x80;
				t = t >> 7;
				t = t << (radix - currentBitNum % radix == radix ? 0 : radix - currentBitNum % radix);
				number += t;
				if (currentBitNum % 14 == 0) {
					// System.out.println("number:"+number);
					char c = codes[number];
					number = 0;

					s.append(c);
				}
			}
		}
		return s.toString();
	}

	public static boolean isEquals(byte[] a, byte[] b) {

		if (a.length != b.length) {
			return false;
		}
		for (int i = 0; i < a.length; i++) {
			if (a[i] != b[i]) {
				return false;
			}
		}
		return true;
	}

	public static byte[] decode(String s) {
		int radix = 14;
		byte[] bytes = new byte[s.length() * radix / 8];
		byte b = 0;
		int currentBitNum = 0;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			short number = charMap.get(c + "");
			// System.out.println(Integer.toBinaryString(number));
			for (int j = 0; j < radix; j++) {
				int t = number << j;
				t = (short) (t & 0x2000);
				t = t >> 13;
				currentBitNum++;
				byte d = (byte) (t << (8 - currentBitNum % 8 == 8 ? 0 : 8 - currentBitNum % 8));
				b = (byte) (b + d);
				if (currentBitNum % 8 == 0) {
					bytes[currentBitNum / 8 - 1] = b;
					b = 0;
				}
			}

		}
		return bytes;
	}

	public static String encode2(byte[] bytes) {
		int radix = 14;
		StringBuilder bits = new StringBuilder();
		StringBuilder s = new StringBuilder();
		for (byte b : bytes) {
			for (int i = 0; i < 8; i++) {
				int t = b << i;
				t = t & 0x80;
				t = t >> 7;
				bits.append(t);
			}
		}
		int length = bytes.length * 8 / radix;
		for (int i = 0; i < length; i++) {
			int start = i * radix;
			int end = (i + 1) * radix;
			String subBits = bits.substring(start, end);
			int index = Integer.parseUnsignedInt(subBits, 2);
			s.append(codes[index]);
		}
		return s.toString();
	}

	public static void main(String[] args) throws UnsupportedEncodingException {

//		byte b = 64;
//		b = (byte) (b + 32);
//		System.out.println(b);
//		// System.out.println(codes.length);
//
//		// int i =Integer.parseUnsignedInt("F7FE", 16);
//
//		// System.out.println(new String(unicode2Bytes(i),"gbk"));
		byte[] bytes = new byte[] {0, 1, 2, 3};
		System.out.println(Arrays.toString(bytes));
		String string = encode(bytes);
		System.out.println(string);
		System.out.println(Arrays.toString(decode(string)));
		
	}

}
