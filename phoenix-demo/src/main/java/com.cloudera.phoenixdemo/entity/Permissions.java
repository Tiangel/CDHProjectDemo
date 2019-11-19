package com.cloudera.phoenixdemo.entity;

import lombok.Data;

@Data
public class Permissions {
    public Permissions() {

    }

    /**
     * 主键
     */
    private Long id;

    private String name;


}