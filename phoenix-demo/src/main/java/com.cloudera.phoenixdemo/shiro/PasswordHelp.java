package com.cloudera.phoenixdemo.shiro;

import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

/*
 *Author:nisan
 *Description:
 *Data:Created in 10:47 2019/06/27
 */
public class PasswordHelp {
    public static String passwordSalt(String userName, Object password) {
        String hashAlgorithmName = "MD5";//加密方式
        ByteSource salt = ByteSource.Util.bytes(userName);//以账号作为盐值
        int hashIterations = 1024;//加密1024次
        return new SimpleHash(hashAlgorithmName, password, salt, hashIterations).toString();
        
    }
    
    public static void main(String[] args) {
        System.out.println(passwordSalt("ztt","12345"));
    }

}
