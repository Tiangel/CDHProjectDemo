package com.cloudera.phoenixdemo.shiro;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.Date;

/*
 *Author:nisan
 *Description:
 *Data:Created in 10:47 2019/06/27
 */
@Component
public class JWTUtil {

    /**
     * 过期时间从配置文件获取
     */

    private static String accessTokenExpireTime;

    @Value("${auth.access.token.expiretime}")
    public void setAccessTokenExpireTime(String accessTokenExpireTime) {
        JWTUtil.accessTokenExpireTime = accessTokenExpireTime;
    }

    /**
     * JWT认证加密私钥(Base64加密)
     */

    private static String encryptJWTKey;

    @Value("${auth.access.encrypt.JWTKey}")
    public void setEncryptJWTKey(String encryptJWTKey) {
        JWTUtil.encryptJWTKey = encryptJWTKey;
    }



    /**
     * 校验token是否正确
     *
     * @param token  密钥
     * @return 是否正确
     */
    public static boolean verify(String token) {
        try {
            String secret = encryptJWTKey + getUsername(token);
            Algorithm algorithm = Algorithm.HMAC256(secret);
            JWTVerifier verifier = JWT.require(algorithm)
                    .build();
            DecodedJWT jwt = verifier.verify(token);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    /**
     * 获得token中的信息无需secret解密也能获得
     *
     * @return token中包含的用户名
     */
    public static String getUsername(String token) {
        try {
            DecodedJWT jwt = JWT.decode(token);
            return jwt.getClaim("userName").asString();
        } catch (JWTDecodeException e) {
            return null;
        }
    }

    /**
     * 获取用户ID
     *
     * @param token
     * @return
     */
    public static Integer getUserId(String token) {
        try {
            DecodedJWT jwt = JWT.decode(token);
            return jwt.getClaim("userId").asInt();
        } catch (JWTDecodeException e) {
            return null;
        }
    }


    /**
     *
     * @param username 用户名
     * @return 加密的token
     */
    public static String sign(Long userId, String username) {
        try {
            String secret = encryptJWTKey + username;
            Date date = new Date(System.currentTimeMillis() + Long.valueOf(accessTokenExpireTime) * 1000);
            Algorithm algorithm = Algorithm.HMAC256(secret);
            // 附带username信息
            return JWT.create()
                    .withClaim("userId", userId)
                    .withClaim("userName", username)
                    .withClaim("currentTimeMillis", System.currentTimeMillis())
                    .withExpiresAt(date)
                    .sign(algorithm);
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException("JWTToken加密出现UnsupportedEncodingException异常:" + e.getMessage());
        }
    }


    public static String getToken() {
        Subject subject = SecurityUtils.getSubject();
        return subject.getPrincipal().toString();
    }


    public static String getUsername() {
        try {
            DecodedJWT jwt = JWT.decode(getToken());
            return jwt.getClaim("username").asString();
        } catch (JWTDecodeException e) {
            return null;
        }
    }


    public static Long getUserId() {
        try {
            DecodedJWT jwt = JWT.decode(getToken());
            return jwt.getClaim("userId").asLong();
        } catch (JWTDecodeException e) {
            return null;
        }
    }



    public static Date getExpiresAt() {
        try {
            DecodedJWT jwt = JWT.decode(getToken());
            return jwt.getExpiresAt();
        } catch (JWTDecodeException e) {
            return null;
        }
    }

    /**
     * sha1 加密
     * @param str
     * @return
     */
    public static String string2Sha1(String str){
        if(str==null||str.length()==0){
            return null;
        }
        char hexDigits[] = {'0','1','2','3','4','5','6','7','8','9',
                'a','b','c','d','e','f'};
        try {
            MessageDigest mdTemp = MessageDigest.getInstance("SHA1");
            mdTemp.update(str.getBytes("UTF-8"));

            byte[] md = mdTemp.digest();
            int j = md.length;
            char buf[] = new char[j*2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                buf[k++] = hexDigits[byte0 >>> 4 & 0xf];
                buf[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(buf);
        } catch (Exception e) {
            return null;
        }
    }

}