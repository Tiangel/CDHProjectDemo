package com.cloudera.phoenixdemo.service;

import com.cloudera.phoenixdemo.utils.ResponseDto;

import java.util.Map;

public interface AuthService {
    /**
     * 开放平台调用
     *
     * @param params
     * @return
     */
    ResponseDto gatewayDo(Map<String, String> params);
}
