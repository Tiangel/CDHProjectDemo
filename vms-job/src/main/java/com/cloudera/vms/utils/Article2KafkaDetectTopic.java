package com.cloudera.vms.utils;

import com.cloudera.vms.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;


public class Article2KafkaDetectTopic {

    private static final Logger logger = LogManager.getLogger(Article2KafkaDetectTopic.class);
    private static final long COMMIT_INTERVAL = 5000;
    private static Producer<String, String> producer = null;
    private static BlockingQueue<Future<RecordMetadata>> queue = new ArrayBlockingQueue<Future<RecordMetadata>>(8192);
    private static long lastCommitTime = 0;

    static {
        Properties props = new Properties();
        String ips = Config.get(Config.KEY_KAFKA_BROKERS);
        String port = Config.get(Config.KEY_KAFKA_PORT);
        String[] ipArray = ips.split(",");
        StringBuilder sb = new StringBuilder();
        for (String ip : ipArray) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(ip + ":" + port);
        }
        logger.info("brokers:" + sb.toString());
        props.put("bootstrap.servers", sb.toString());
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        props.put("batch.num.messages", "1000");
        producer = new KafkaProducer<String, String>(props);
        new Thread() {

            @Override
            public void run() {
                List<Future<RecordMetadata>> sends = new ArrayList<Future<RecordMetadata>>();
                while (true) {
                    try {
                        int size = queue.size();
                        Future<RecordMetadata> send = queue.poll();
                        long now = Calendar.getInstance().getTimeInMillis();
                        if (null != send) {
                            sends.add(send);
                            if (sends.size() > 400 || now - lastCommitTime > 5000) {
                                for (Future<RecordMetadata> s : sends) {
                                    try {
                                        s.get();
                                    } catch (Exception e) {
                                        logger.info("send msg to article_detect_data failed");
                                        e.printStackTrace();
                                    }

                                }
                                sends = new ArrayList<Future<RecordMetadata>>();
                                lastCommitTime = Calendar.getInstance().getTimeInMillis();
                                logger.info("####es_data2 queue size:" + size);
                            }
                        } else {
                            if (sends.size() > 0 && now - lastCommitTime > 5000) {
                                for (Future<RecordMetadata> s : sends) {
                                    try {
                                        s.get();
                                    } catch (Exception e) {
                                        logger.info("send msg to article_detect_data failed");
                                        e.printStackTrace();
                                    }
                                }
                                sends = new ArrayList<Future<RecordMetadata>>();
                                lastCommitTime = Calendar.getInstance().getTimeInMillis();
                                logger.info("####es_data queue size:" + size);
                            }
                            Thread.sleep(500);
                        }

                    } catch (Throwable e) {

                        logger.info("#########commit to kafka article_detect_data error");
                        e.printStackTrace();
                    }
                }
            }


        }.start();
    }


    /**
     * 普通队列
     * @param news
     */
    public static void sendNews(String news) {
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(Config.get(Config.KEY_KAFKA_TOPIC_DETECT), news);
        Future<RecordMetadata> send = producer.send(data);
        try {
            queue.put(send);
        } catch (Exception e) {
            logger.info("send messages to kafka topic article_detect_data failed");
            e.printStackTrace();

        }
    }


    /**
     * 发送到优先队列 article_detect_data_priority1
     *
     * @param news
     */
    public static void sendNews_priority1(String news) {
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(Config.get(Config.KEY_KAFKA_TOPIC_DETECT) + "_priority1", news);
        Future<RecordMetadata> send = producer.send(data);
        try {
            queue.put(send);
        } catch (Exception e) {
            logger.info("send messages to kafka topic article_detect_data failed");
            e.printStackTrace();

        }
    }


    public static void close() {
        producer.close();
    }

}