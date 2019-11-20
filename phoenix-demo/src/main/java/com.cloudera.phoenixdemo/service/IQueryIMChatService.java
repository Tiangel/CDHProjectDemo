package com.cloudera.phoenixdemo.service;

import com.cloudera.phoenixdemo.utils.ResponseDto;

import java.util.Map;

public interface IQueryIMChatService {

    ResponseDto queryIMChatFromES(Map<String, Object> map);
}
