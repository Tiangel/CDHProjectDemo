package com.cloudera.phoenixdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.phoenixdemo.constant.IMChatConstant;
import com.cloudera.phoenixdemo.service.IKafkaConsumerListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Service
public class KafkaConsumerListener implements IKafkaConsumerListener {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @Autowired
    private RestHighLevelClient highLevelClient;

    @KafkaListener(id = "test1", topics = "#{'${kafka.consumer.topics}'.split(',')}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String content = record.value();
            logger.info("begin offset ====> " + record.offset());
            System.out.println("====" + content);
            boolean handle = this.handle(content);
            if (handle) {
                // 直接提交 offset
                ack.acknowledge();
            }
            logger.info("end offset ====> " + record.offset());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * 批量消费 kafka
     */
    @KafkaListener(id = "test", topics = "#{'${kafka.consumer.topics}'.split(',')}", containerFactory = "kafkaListenerContainerFactory")
    public void consumeList(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        logger.info("=============== Total " + records.size() + " events in this batch ..");
        try {
            List<String> list = new ArrayList<String>();
            for (ConsumerRecord<String, String> record : records) {
                Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                if (kafkaMessage.isPresent()) {
                    String message = record.value();
                    list.add(message);
                    String topic = record.topic();
                    logger.info("message = {}",  message);
                }
            }
            boolean handle = this.bulkHandle(list);
            if (handle) {
                // 直接提交 offset
                ack.acknowledge();
            }
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


    /**
     * 批量插入ES
     */
    private boolean bulkHandle(List<String> list) {
        BulkRequest bulkRequest = new BulkRequest();
        boolean flag = true;
        try {
            for (String content : list) {
                JSONObject jsonObject = JSON.parseObject(content);
                String msg_id = (String) jsonObject.get("msg_id");
                IndexRequest request = new IndexRequest(IMChatConstant.COMMUNITY_IM_CHAT_INDEX);
                request.id(msg_id);
                request.source(content, XContentType.JSON);
                bulkRequest.add(request);
            }
            BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logger.error("批量写入ES成功...   " + System.currentTimeMillis());
            }else{
                logger.error("批量写入ES失败...   " + System.currentTimeMillis());
                flag = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("批量写入ES异常 ===>" + e.getMessage());
            flag = false;
        }
        return flag;
    }
}
