package com.cloudera.phoenixdemo.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * <p>
 * 
 * </p>
 *
 * @author zhangxin
 * @since 2019-07-01
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class Role implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private String description;

    /**
     * 角色类型：0普通角色,1管理员角色
     */
    private String type;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;


}
