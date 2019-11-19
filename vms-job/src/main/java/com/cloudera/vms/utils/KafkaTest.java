package com.cloudera.vms.utils;

import com.cloudera.vms.Config;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

//import kafka.cluster.BrokerEndPoint;

//import kafka.cluster.BrokerEndPoint;

//import kafka.cluster.BrokerEndPoint;

//import kafka.cluster.BrokerEndPoint;


public class KafkaTest {


    private static List<String> m_replicaBrokers = new ArrayList<String>();


    private static PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {

                consumer = new SimpleConsumer(seed, a_port, 10000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);

                TopicMetadataRequest req = new TopicMetadataRequest(topics);

                kafka.javaapi.TopicMetadataResponse resp = null;
                try {
                    resp = consumer.send(req);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("发送请求错误："+e);

                }

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            //kafka-2.10_0.8.2.1(上海）
            //for (Broker replica : returnMetaData.replicas()) {
            //kafka-2.10_0.10.0.0.0（华为云）
            for (BrokerEndPoint replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

/*
    public static void consume() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "218.17.83.27:443");
        //props.put("zookeeper.connect", "test12:2181");

        //group 代表一个消费组
        props.put("group.id", "k8k1");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<String> list=new ArrayList<>();
        list.add("topic1");
        //kafka0.10版本
        consumer.subscribe(list);
        //kafka0.8版本
       // consumer.subscribe("topic1");
        while (true) {

            //ConsumerRecords<String, String> records = consumer.poll(1000);
            //Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
            Map<TopicPartition, ConsumerRecords<String, String>> records = (Map<TopicPartition, ConsumerRecords<String, String>>) consumer.poll(100);
            for (ConsumerRecords<String, String> record : records.values()) {

                //List<ConsumerRecord<String, String>> messages = record.records(0, 1, 2);
                List<ConsumerRecord<String, String>> messages = record.records(1);
                for (ConsumerRecord<String, String> message : messages) {
                    System.out.printf("partition=%d,offset = %d, key = %s, value = %s", message.partition(), message.offset(), message.key(), message.value());
                    System.out.println();
                }

                // consumer.commitSync();
            }


            //Thread.sleep(100000);

        }
    }
*/

    public static void produce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "test11:9092");

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 1; i++) {
            Future<RecordMetadata> send1 = producer.send(new ProducerRecord<String, String>("topic1", Integer.toString(i), "1_482877130701835|52,53,54,76|【日本学生上海遇茶托喝“48元一口茶”千元现金被强行掏空】网友爆料，6日，两名日本学生前往上海豫园游玩，被茶托带到茶馆喝茶。“结账时被告知茶费是48元一口。共消费了2100多元，结果二人的现金1000多元全部被强行掏空。”警方介入后钱被退回，监管部门对涉事茶楼进行检查。http://url.cn/2Iak8S5")
            );

            try {
                send1.get();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        producer.close();
    }

    public static void produce(String topic, List<String> articles) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "test11:9092");

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < articles.size(); i++)
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), articles.get(i)));

        producer.close();
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static Map<Integer, Long> getEarlistOffset(String topic, int totalPartitions) {

        Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();
        List<String> brokers = Arrays.asList(Config.get(Config.KEY_KAFKA_BROKERS).split(","));
        int port = Integer.parseInt(Config.get(Config.KEY_KAFKA_PORT));

        for (int i = 0; i < totalPartitions; i++) {

            try {
                PartitionMetadata partitionMetadata = findLeader(brokers, port, topic, i);

                String leadBroker = partitionMetadata.leader().host();

                SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 5000, 64 * 1024, "consumer");

                long offset = getLastOffset(consumer, topic, i, kafka.api.OffsetRequest.EarliestTime(), "consumer");
                if (null != consumer) {
                    consumer.close();
                }

                partitionOffsets.put(i, offset);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("findLeader："+e);
            }


        }
        return partitionOffsets;
    }


    public static Map<Integer, Long> getLatestOffset(String topic, int totalPartitions) {

        Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();
        List<String> brokers = Arrays.asList(Config.get(Config.KEY_KAFKA_BROKERS).split(","));
        int port = Integer.parseInt(Config.get(Config.KEY_KAFKA_PORT));
        for (int i = 0; i < totalPartitions; i++) {
            PartitionMetadata partitionMetadata = findLeader(brokers, port, topic, i);

            String leadBroker = partitionMetadata.leader().host();
            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 5000, 64 * 1024, "consumer");
            long offset = getLastOffset(consumer, topic, i, kafka.api.OffsetRequest.LatestTime(), "consumer");
            if (null != consumer) {
                consumer.close();
            }

            partitionOffsets.put(i, offset);


        }
        return partitionOffsets;
    }


    public static void main(String[] args) throws InterruptedException {
        //produce();
        //produce("weibo", null);
        // consume();
//		List<String> seedBrokers = new ArrayList<String>();
//		seedBrokers.add("test11");
//		seedBrokers.add("test12");
//		seedBrokers.add("test13");
//		PartitionMetadata partitionMetadata = findLeader(seedBrokers, 9092, "weibo-simple2", 2);
//		String leadBroker = partitionMetadata.leader().host();
//		   SimpleConsumer consumer = new SimpleConsumer(leadBroker, 9092, 100000, 64 * 1024, "consumer");
//		long offset = getLastOffset(consumer,"weibo-simple2",2,kafka.api.OffsetRequest.EarliestTime(),"consumer");
//		System.out.println(offset);
        Map<Integer, Long> map = getEarlistOffset("exclude_weibo_data", 4);
        System.out.println(map);
//        getLatestOffset("hanming_data", 4);
    	
    }

}
