package com.cloudera.phoenixdemo.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class Permission implements Serializable {
    /**
     * 主键
     */
    private Long id;

    /**
     * 归属菜单,前端判断并展示菜单使用,
     */
    private String menuCode;

    /**
     * 菜单的中文释义
     */
    private String menuName;

    /**
     * 权限的代码/通配符,对应代码中@RequiresPermissions 的value
     */
    private String permissionCode;

    /**
     * 本权限的中文释义
     */
    private String permissionName;

    /**
     * 操作者
     */
    private Long operator;

    /**
     * 创建日期
     */
    private Date createTime;

    /**
     * 更新日期
     */
    private Date updateTime;

    /**
     * 状态 0废除，1激活
     */
    private Integer deleteStatus;

}