package com.cloudera.phoenixdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.phoenixdemo.constant.IMChatConstant;
import com.cloudera.phoenixdemo.service.CommunityIMSchedule;
import com.cloudera.phoenixdemo.utils.DateUtils;
import com.cloudera.phoenixdemo.utils.HttpClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class CommunityIMScheduleImpl implements CommunityIMSchedule {

    private static Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);

    @Value("${hdsc.im.grant.type}")
    private String grantType;
    @Value("${hdsc.im.client.id}")
    private String clientId;
    @Value("${hdsc.im.client.secret}")
    private String clientSecret;
    @Value("${hdsc.im.orgname}")
    private String orgName;
    @Value("${hdsc.im.appname}")
    private String appName;
    @Value("${hdsc.im.resource.uri}")
    private String uri;
    @Value("${hdsc.im.download.path}")
    private String path;

    private static final String IM_CRON = "0 15,30,45 * * * *";


    /**
     * 调用环新信聊天记录接口
     */
    @Scheduled(cron = IM_CRON)
    public void runIMChatFileTask() {
        String token = getToken();
//        String time = "2019110410";
        String time = DateUtils.getAppointDayHour("yyyyMMddHH", -2);
        logger.info("开始执行调用环新信聊天记录接口任务 ===> 开始时间: " + DateUtils.getCurrentTime("yyyy-MM-dd HH:mm:ss") + "请求下载参数: " + time);
        List<Map<String, String>> chatmessages = getChatmessages(token, time);
        downloadChatFile(chatmessages, time);
    }

    public void downloadChatFile(List<Map<String, String>> list, String fileName) {
        // 经环信IM确认,一小时只生成一个压缩文件
        if (list != null && list.size() > 0) {
            String url = list.get(0).get("url");
            HttpClientUtil.downloadGZFile(url,path + fileName);
        }
    }

    private List<Map<String, String>> getChatmessages(String token, String time) {
        Map<String, String> header = new HashMap<>();
        header.put("Accept", IMChatConstant.APPLICATION_JSON);
        header.put("Authorization", "Bearer " + token);
        String url = uri + "/" + orgName + "/" + appName + "/chatmessages/" + time;
        String result = HttpClientUtil.doGet(url, header, null);
        logger.info("调用环信聊天记录接口: " + result);
        List<Map<String, String>> data = new ArrayList<Map<String, String>>();
        if (StringUtils.isNoneBlank(result)) {
            JSONObject jsonObject = JSONObject.parseObject(result);
            boolean error = jsonObject.containsKey("error");
            if (!error) {
                data = (List<Map<String, String>>) jsonObject.get("data");
            }
        }
        return data;
    }

    private String getToken() {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("Content-Type", IMChatConstant.APPLICATION_JSON);
        paramMap.put("grant_type", grantType);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        String json = JSON.toJSONString(paramMap);
        String url = uri + "/" + orgName + "/" + appName + "/token";
        String result = HttpClientUtil.doPostJson(url, json);
        logger.info("调用环信获取token接口: " + result);
        JSONObject jsonObject = JSONObject.parseObject(result);
        String token = (String) jsonObject.get("access_token");
        return token;
    }
}
