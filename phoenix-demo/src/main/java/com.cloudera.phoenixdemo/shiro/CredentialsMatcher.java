package com.cloudera.phoenixdemo.shiro;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;

/*
 *Author:nisan
 *Description:
 *Data:Created in 10:47 2019/06/27
 */
public class CredentialsMatcher extends SimpleCredentialsMatcher {
    
    
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {

        
        return super.doCredentialsMatch(token,info);
}
    
}