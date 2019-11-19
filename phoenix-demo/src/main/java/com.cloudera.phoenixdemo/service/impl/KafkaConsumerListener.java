package com.cloudera.phoenixdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hdjt.bigdata.Constant.IMChatConstant;
import com.hdjt.bigdata.service.IKafkaConsumerListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerListener implements IKafkaConsumerListener {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @Autowired
    private RestHighLevelClient highLevelClient;

    @KafkaListener(id = "test1", topics = "#{'${kafka.consumer.topics}'.split(',')}", containerFactory = "kafkaListenerContainerFactory")
    public void consumer(ConsumerRecord<String, String> record) {
        try {
            String content = record.value();
            logger.info("begin offset ====> " + record.offset());
            System.out.println("====" + content);
            this.handle(content);
            logger.info("end offset ====> " + record.offset());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public boolean handle(String content) {
        JSONObject jsonObject = JSON.parseObject(content);
        String msg_id = (String) jsonObject.get("msg_id");
        boolean flag = true;
        try {
            IndexRequest request = new IndexRequest(IMChatConstant.COMMUNITY_IM_CHAT_INDEX);
            request.id(msg_id);
            request.source(content, XContentType.JSON);
            IndexResponse response = highLevelClient.index(request, RequestOptions.DEFAULT);
//            异步处理
//            highLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>(){
//                @Override
//                public void onResponse(IndexResponse indexResponse) {
//                    logger.info("写入ES成功, 消息id ===>" + msg_id);
//                }
//                @Override
//                public void onFailure(Exception e) {
//                    logger.error("写入ES失败, 消息id ===>" + msg_id);
//                }
//            });
            System.out.println("IndexResponse===> "+ response);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("msg_id [" + msg_id + "]写入ES异常 ===>" + e.getMessage());
            flag = false;
        }
        return flag;
    }
}
