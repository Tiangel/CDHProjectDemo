package com.cloudera.vms.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Md5Utils {
    private static ThreadLocal<MessageDigest> threadLocal = new ThreadLocal<MessageDigest>(){
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }
    };

    public static MessageDigest getMD() {
        return threadLocal.get();
    }

	/*private static MessageDigest md = null;
	static{
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
	}*/
    private final static String[] charDigits = { "0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };

    private static String byteToArrayString(byte bByte) {
        int iRet = bByte;
        if (iRet < 0) {
            iRet += 256;
        }
        int iD1 = iRet / 16;
        int iD2 = iRet % 16;
        return charDigits[iD1] + charDigits[iD2];
    }

    /**
     * 转换字节数组为16进制字串
     * @param bytes
     * @return
     */
    private static String byteToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(byteToArrayString(bytes[i]));
        }
        return sb.toString();
    }

    public static String getMd5ByStr(String str) {
            //return  byteToString(md.digest(str.getBytes()));
            return  byteToString(getMD().digest(str.getBytes()));
    }
    public static String getMd5ByBytes(byte[] bytes) {
        //return  byteToString(md.digest(bytes));
        return  byteToString(getMD().digest(bytes));
    }
    public static void main(String[] args) {
    	System.out.println(Md5Utils.getMd5ByStr("http://nj.fangtan007.com/xuanwu/chushou/fangwu/p2/"));
    	//System.out.println(UUID.nameUUIDFromBytes("359441051550393".getBytes()));
	}
}
