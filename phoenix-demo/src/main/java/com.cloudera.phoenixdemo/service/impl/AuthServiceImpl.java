package com.cloudera.phoenixdemo.service.impl;

import com.hdjt.bigdata.service.AuthService;
import com.hdjt.bigdata.utils.ResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class AuthServiceImpl implements AuthService {

    @Override
    public ResponseDto gatewayDo(Map<String, String> params) {
        String app_key = params.get("app_key");
        String sign = params.get("sign");

        return null;
    }
}
