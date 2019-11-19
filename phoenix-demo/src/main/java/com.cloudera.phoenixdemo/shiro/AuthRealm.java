package com.cloudera.phoenixdemo.shiro;


import com.hdjt.bigdata.mapper.auth.UserMapper;
import com.hdjt.bigdata.mapper.auth.entity.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 自定义Realm
 */
@Service
public class AuthRealm extends AuthorizingRealm {

    @Autowired
    private  UserMapper userMapper;

    @Override
    public boolean supports(AuthenticationToken authenticationToken) {
        return authenticationToken instanceof JWTToken;
    }



    /**
     * 用户权限判断
     * @param principalCollection
     * @return
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principalCollection) {
        SimpleAuthorizationInfo simpleAuthorizationInfo = new SimpleAuthorizationInfo();
        Integer userId = JWTUtil.getUserId(principalCollection.toString());
        //查询用户权限
        List<User> userPermission = userMapper.getPermission(userId);
        for (User user : userPermission) {
            simpleAuthorizationInfo.addStringPermission(user.getUserPermission());
        }
        return simpleAuthorizationInfo;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken authenticationToken) throws AuthenticationException{
        String token = (String) authenticationToken.getCredentials();
        // 解密获取username 用户数据集比较
        String username = JWTUtil.getUsername(token);
        //账户为空
        if (StringUtils.isBlank(username)) {
            throw new AuthenticationException("token 验证失败");
        }

        User user = userMapper.getInfo(username);

        if (user == null || user.getStatus() != 1) {
            throw new AuthenticationException("无效用户");
        }

        // 认证token
        if (JWTUtil.verify(token)) {
            return new SimpleAuthenticationInfo(token,token,"authRealm");
        }
        throw new AuthenticationException("token 验证失败");
    }


}