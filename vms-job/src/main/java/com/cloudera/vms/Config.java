package com.cloudera.vms;

import java.io.IOException;
import java.util.Properties;

public class Config {
	
	public static final String KEY_KAFKA_BROKERS = "kafka.brokers";
	public static final String KEY_KAFKA_PORT = "kafka.port";
	public static final String KEY_ES_NODES="es.nodes";
	public static final String KEY_ES_PORT = "es.port";
	public static final String KEY_ES_CLUSTER_NAME = "es.cluster.name";
	public static final String KEY_CLUSTING_TOP_NUM = "clusting.top.number";
	public static final String KEY_CLUSTINGE_DECAY = "clusting.decay";
	public static final String KEY_ES_WEIBO_URL = "es.weibo.url";
	public static final String KEY_ES_ARTICLE_URL = "es.article.url";
	public static final String KEY_ES_SUBJECT_URL = "es.subject.url";
	public static final String KEY_HBASE_WEIBO_NAME = "hbase.weibo.name";
	public static final String KEY_HBASE_ARTICLE_NAME = "hbase.article.name";
	public static final String KEY_HBASE_SUBJECT_NAME = "hbase.subject.name";
	public static final String KEY_HBASE_HOTTOPIC_NAME = "hbase.hottopic.name";
	public static final String KEY_CLUSTING_GROUP_GAP = "clusting.group.gap";
	public static final String KEY_JOB_CHECKPOINT_URL = "job.checkpoint.url";
	
	public static final String KEY_KAFKA_TOPIC_DETECT = "kafka.topic.detect";
	public static final String KEY_KAFKA_TOPIC_HANMINGCODE = "kafka.topic.hanmingCode";
	public static final String KEY_KAFKA_TOPIC_UPDATE = "kafka.topic.update";
	public static final String KEY_KAFKA_TOPIC_ALERT_AMS = "kafka.topic.alert.ams";
	public static final String KEY_KAFKA_HAMINGResult="kafka.topic.hanmingResult";
	public static final String KAFKA_SERVERS="kafka.servers";
	
	public static final String KEY_REDIS_URL = "redis.url";
	
//	public static Map<String,String> configs = new HashMap<String,String>();
	static Properties pps = new Properties();
	
	
	static{
		 try {
			pps.load(Config.class.getResourceAsStream("/env.properties"));
//			configs.put(KEY_KAFKA_BROKERS, pps.getProperty(KEY_KAFKA_BROKERS));
//			configs.put(KEY_KAFKA_PORT, pps.getProperty(KEY_KAFKA_PORT));
//			configs.put(KEY_CLUSTING_TOP_NUM, pps.getProperty(KEY_CLUSTING_TOP_NUM));
//			configs.put(KEY_CLUSTINGE_DECAY, pps.getProperty(KEY_CLUSTINGE_DECAY));
//			configs.put(KEY_ES_NODES, pps.getProperty(KEY_ES_NODES));
//			configs.put(KEY_ES_PORT, pps.getProperty(KEY_ES_PORT));
//			configs.put(KEY_ES_CLUSTER_NAME, pps.getProperty(KEY_ES_CLUSTER_NAME));
//			configs.put(KEY_ES_WEIBO_URL, pps.getProperty(KEY_ES_WEIBO_URL));
//			configs.put(KEY_HBASE_WEIBO_NAME, pps.getProperty(KEY_HBASE_WEIBO_NAME));
//			configs.put(KEY_HBASE_HOTTOPIC_NAME, pps.getProperty(KEY_HBASE_HOTTOPIC_NAME));
//			configs.put(KEY_CLUSTING_GROUP_GAP, pps.getProperty(KEY_CLUSTING_GROUP_GAP));
//			configs.put(KEY_JOB_CHECKPOINT_URL, pps.getProperty(KEY_JOB_CHECKPOINT_URL));
//			configs.put(KEY_HBASE_SUBJECT_NAME, pps.getProperty(KEY_HBASE_SUBJECT_NAME));
//			configs.put(KEY_ES_SUBJECT_URL, pps.getProperty(KEY_ES_SUBJECT_URL));
//			configs.put(KEY_ES_ARTICLE_URL, pps.getProperty(KEY_ES_ARTICLE_URL));
//			configs.put(KEY_HBASE_ARTICLE_NAME, pps.getProperty(KEY_HBASE_ARTICLE_NAME));
//			
//			configs.put(KEY_KAFKA_TOPIC_DETECT, pps.getProperty(KEY_KAFKA_TOPIC_DETECT));
//			configs.put(KEY_KAFKA_TOPIC_HANMINGCODE, pps.getProperty(KEY_KAFKA_TOPIC_HANMINGCODE));
//			configs.put(KEY_KAFKA_TOPIC_UPDATE, pps.getProperty(KEY_KAFKA_TOPIC_UPDATE));
//			configs.put(KEY_REDIS_URL, pps.getProperty(KEY_REDIS_URL));
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String get(String key){
		return pps.getProperty(key);
	}
	
	public static void main(String[] args){
		System.out.println(Config.get(KEY_KAFKA_TOPIC_HANMINGCODE));
	}
	

}
