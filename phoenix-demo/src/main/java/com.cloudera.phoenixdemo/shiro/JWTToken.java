package com.cloudera.phoenixdemo.shiro;

import org.apache.shiro.authc.UsernamePasswordToken;

/*
*Author:nisan
*Description:
*Data:Created in 10:47 2019/06/27
*/
public class JWTToken extends UsernamePasswordToken {
	
	// 密钥
	private String token;
	
	public JWTToken(String token) {
		this.token = token;
	}
	
	@Override
	public Object getPrincipal() {
		return token;
	}
	
	@Override
	public Object getCredentials() {
		return token;
	}
}