package com.cloudera.phoenixdemo.controller;

import com.alibaba.fastjson.JSON;
import com.cloudera.phoenixdemo.constant.IMChatConstant;
import com.cloudera.phoenixdemo.service.IQueryIMChatService;
import com.cloudera.phoenixdemo.utils.ResponseDto;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/imchat")
public class ImChatontroller {

    @Autowired
    private RestHighLevelClient highLevelClient;

    @Autowired
    private IQueryIMChatService queryIMChatService;

    private static Logger logger = LoggerFactory.getLogger(ImChatontroller.class);


    /**
     * 根据id获取ES对象
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseDto getIMChatById(@PathVariable("id") String id) {
        GetResponse getResponse = null;
        String responseBody = null;
        GetRequest getRequest = new GetRequest(IMChatConstant.COMMUNITY_IM_CHAT_INDEX, id);
        try {
            getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                responseBody = getResponse.getSourceAsString();
            } else {
                responseBody = "can not found the community_im_chat by your id";
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new ResponseDto(e);
        }
        return new ResponseDto(responseBody);
    }


    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public ResponseDto query(@RequestBody Map<String, Object> map) {

        Map<String, Object> queryMap = new HashMap<>();
        Map<String, Object> queryAndCondition = new HashMap<>();
        Map<String, Object> queryOrCondition = new HashMap<>();
        String msg_id = (String) map.get("msg_id");
        if (StringUtils.isNoneBlank(msg_id)) {
            queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_MSG_ID, msg_id);
        }

        String userId = (String) map.get("userId");
        if (StringUtils.isNoneBlank(userId)) {
            String roleType = (String) map.get("roleType");
            if (IMChatConstant.IM_CHAT_QUERY_ROLETYPE_ALL.equalsIgnoreCase(roleType)) {
                queryOrCondition.put(IMChatConstant.IM_CHAT_QUERY_FROM, userId);
                queryOrCondition.put(IMChatConstant.IM_CHAT_QUERY_TO, userId);
            } else if (IMChatConstant.IM_CHAT_QUERY_ROLETYPE_HKEEPER.equalsIgnoreCase(roleType)) {
                queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_TO, userId);
            } else if (IMChatConstant.IM_CHAT_QUERY_ROLETYPE_RESIDENT.equalsIgnoreCase(roleType)) {
                queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_FROM, userId);
            } else {
                queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_FROM, userId);
            }
        }

        String chat_type = (String) map.get(IMChatConstant.IM_CHAT_QUERY_CHAT_TYPE);
        if (StringUtils.isNoneBlank(chat_type)) {
            queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_CHAT_TYPE, chat_type);
        }

        String mediaType = (String) map.get("mediaType");
        if (StringUtils.isNoneBlank(mediaType)) {
            queryAndCondition.put(IMChatConstant.IM_CHAT_MEDIA_TYPE, mediaType);
        }

        String msg = (String) map.get("msg");
        if (StringUtils.isNoneBlank(msg)) {
            queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_MSG, msg);
        }

        Map<String, Object> timestamp = new HashMap<>();
        timestamp.put(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_BEGINDATE, map.get(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_BEGINDATE));
        timestamp.put(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_ENDDATE, map.get(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_ENDDATE));
        queryAndCondition.put(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP, timestamp);


        Object pageNo = map.get("pageNo");
        int fromNum = 0;
        int querySize = 0;
        if(null != pageNo){
            fromNum = (int) pageNo -1;
        }else{
            fromNum = IMChatConstant.COMMUNITY_IM_CHAT_DEFAULT_FROM_NUM;
        }
        Object pageSize = map.get("pageSize");
        if(null != pageSize){
            querySize = (int) pageSize;
        }else{
            querySize = IMChatConstant.COMMUNITY_IM_CHAT_DEFAULT_PAGE_SIZE;
        }
        queryMap.put(IMChatConstant.COMMUNITY_IM_CHAT_FROMNUM, fromNum * querySize);
        queryMap.put(IMChatConstant.COMMUNITY_IM_CHAT_QUERYSIZE, pageSize);
        queryMap.put(IMChatConstant.COMMUNITY_IM_CHAT_QUERY_AND_CONDITION, queryAndCondition);
        queryMap.put(IMChatConstant.COMMUNITY_IM_CHAT_QUERY_OR_CONDITION, queryOrCondition);
        queryMap.put(IMChatConstant.COMMUNITY_IM_CHAT_ORDERBY_CLAUSE, map.get(IMChatConstant.COMMUNITY_IM_CHAT_ORDERBY_CLAUSE));

        System.out.println(JSON.toJSONString(queryMap));
        return queryIMChatService.queryIMChatFromES(queryMap);
    }

    // {"queryOrCondition":{"from":"715cb167df8b990a7748cd92402224f3","to":"715cb167df8b990a7748cd92402224f3"},"queryAndCondition":{"timestamp":{"beginDate":1573694783000,"endDate":1574048112285}},"querySize":100,"orderByClause":{"msg_id":true,"timestamp":false},"fromNum":0}
    @RequestMapping(value = "/queryES", method = RequestMethod.POST)
    public ResponseDto queryES(@RequestBody Map<String, Object> map) {
        return queryIMChatService.queryIMChatFromES(map);
    }
}