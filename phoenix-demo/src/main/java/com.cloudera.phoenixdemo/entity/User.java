package com.cloudera.phoenixdemo.entity;

import java.util.Date;


public class User {
    /**
     * 主键
     */
    private Integer id;


    private String userName;

    public Integer getId() {

        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String userPassword) {
        this.password = userPassword == null ? null : userPassword.trim();
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName == null ? null : userName.trim();
    }


    /**
     * 登录密码
     */
    private String password;


    /**
     * 创建日期
     */
    private Date createdAt;

    /**
     * 更新日期
     */
    private Date updateAt;

    /**
     * 状态 0废除，1激活
     */
    private Integer status;
    public Integer getStatus() {
        return status;
    }

    private String userPermission;
    public void setUserPermission(String userPermission) {
        this.userPermission = userPermission == null ? null : userPermission.trim();
    }
    public String getUserPermission() {
        return userPermission;
    }

}