package com.cloudera.vms.test;//package com.izhonghong.vms.test;
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import java.sql.Timestamp;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Calendar;
//import java.util.Collection;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//import java.util.HashMap;
//import java.util.regex.Pattern;
//
//import kafka.common.TopicAndPartition;
//import kafka.message.MessageAndMetadata;
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//
////import org.apache.spark.streaming.kafka010.ConsumerStrategies;
////import org.apache.spark.streaming.kafka010.KafkaUtils;
////import org.apache.spark.streaming.kafka010.LocationStrategies;
////import org.apache.spark.streaming.kafka010.OffsetRange;
////import org.apache.spark.streaming.kafka010.HasOffsetRanges;
//
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.OffsetRange;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.izhonghong.vms.bean.Article;
//import com.izhonghong.vms.bean.ArticleModifier;
//import com.izhonghong.vms.utils.Article2KafkaDetectTopic;
//import com.izhonghong.vms.utils.Article2KafkaHanmingCodeTopic;
//import com.izhonghong.vms.utils.ArticleModifier2KafkaTopic;
//import com.izhonghong.vms.utils.JedisClusterUtils;
//import com.izhonghong.vms.utils.KafkaTest;
//import com.izhonghong.vms.utils.Md5Utils;
//import com.izhonghong.vms.zookeeper.ZKUtils;
//
//
//
//
//
///**
// * Consumes messages from topic in Kafka and distribute the articles to other topics
// *
// * Usage: ArticleDistributionJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>
// *   <zkQuorums> is a list of one or more zookeeper servers that make quorum
// *   <group> is the name of kafka consumer group
// *   <topic> is a  kafka topic to consume from
// *   <totalPartitions> total partitions of the topic
// *   <numThreads> is the number of threads the kafka consumer should use
// *   <duration>   The time interval at which streaming data will be divided into batches
// * To run this example:
// *   sudo ./spark-submit  --class HanmingCodeComputeJob  --master spark://test11:7077  --executor-memory 4G --total-executor-cores 10 --jars (jars separated by ",") /home/izhonghong/test/spark-test/spark-test-0.0.1-SNAPSHOT.jar test11:2181,test12:2181,test13:2181 g3 weibo-simple2 4 4 5000
// */
//
//public  class ArticleDistributionJobTest {
//
//  private static final Pattern SPACE = Pattern.compile(" ");
//  private static final Logger logger =  LogManager.getLogger(ArticleDistributionJobTest.class);
//  private static DateFormat df = new SimpleDateFormat("yyyyMMdd");
//  private static DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//  private static int maxSendSize = 50;
//
//  private ArticleDistributionJobTest() {
//  }
//
//  @SuppressWarnings("deprecation")
//  public static void main(String[] args) throws Exception {
//	  if (args.length < 6) {
//	      System.err.println("Usage: ArticleDistributionJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
//	      System.exit(1);
//	  }
//
//
//    SparkConf sparkConf = new SparkConf().setAppName("ArticleDistributionJob");;
//    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true") ;
//
//    //kafka集群列表
//    String brokerList = args[0];
//    //消费组
//    final String group = args[1];
//    //消费topic
//    String topic = args[2];
//    //partition数量
//    int totalPartitions = Integer.parseInt(args[3]);
//    //
//    int numThreads = Integer.parseInt(args[4]);
//    int duration = Integer.parseInt(args[5]);
//
//    // Create the context with 2 seconds batch size
//   //sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
//    final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
//    //jssc.sc().addJar("hdfs:///app/hduser1301/lib/hbase-client-1.2.2.jar");
//    Map<String, Integer> topicMap = new HashMap<String, Integer>();
//
//    topicMap.put(topic, numThreads);
//
//
//
//    Map<String, String> kafkaParams = new HashMap<String, String>();
//    kafkaParams.put("metadata.broker.list",brokerList);
//
//
//    kafkaParams.put("group.id", group);
//
//    kafkaParams.put("session.timeout.ms","30000");
//    kafkaParams.put("request.timeout.ms", "60000");
//
//
//    java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets = new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
//    Map<Integer,Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
//    Map<Integer,Long> earlistOffsets = null;
//    try{
//    	earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
//    }catch(Exception e){
//    	logger.error("failed to get the partitions' offset, the topic:"+topic+" may doesn't exist");
//    }
//
//    if(null!=partitionOffsets&&partitionOffsets.size()>0){
//    	 for(Integer partition:partitionOffsets.keySet()){
//    		    long offset = partitionOffsets.get(partition);
//    		    long earlistOffset = earlistOffsets.get(partition);
//    		    logger.info("partition:"+partition+",offset:"+offset+",earlistOffset:"+earlistOffset);
//    		    System.out.println("partition:"+partition+",offset:"+offset+",earlistOffset:"+earlistOffset);
//    		    if(earlistOffset>offset){
//    		    	offset=earlistOffset;
//    		    }
//    	    	fromOffsets.put(new TopicAndPartition(topic,partition), offset);
//    	    }
//    }else{
//    	for(int i=0;i<totalPartitions;i++){
//    		fromOffsets.put(new TopicAndPartition(topic,i), earlistOffsets.get(i));
//    	}
//    }
//    System.out.println("kafkaParams:"+kafkaParams);
//
//
//
//   //create the direct stream, with witch we can maintain the offset in zookeeper by ourselves
//    JavaInputDStream<Row> stream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, Row.class, kafkaParams, fromOffsets,
//    		 new Function<MessageAndMetadata<String, String>, Row>() {
//                 public Row call(MessageAndMetadata<String, String> v1)
//                         throws Exception {
//                	 String message = v1.message();
//
//                     return RowFactory.create(message);
//                 }
//             });
//
//
//   	//final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jssc.sc());
//
//
//
//    System.out.println("=========================lines count:"+stream.count());
//
//
////
//   	stream.foreachRDD(new Function<JavaRDD<Row>, Void> (){
//
//		public Void call(JavaRDD<Row> jrdd) throws Exception {
//			  OffsetRange[] offsets = ((HasOffsetRanges) jrdd.rdd()).offsetRanges();
//
//			  jrdd = jrdd.filter(new Function<Row,Boolean>(){//去除空的message
//
//					public Boolean call(Row v1) throws Exception {
//						//System.out.println("article :"+v1.get(0));
//						return v1!=null&&v1.get(0)!=null;
//					}
//			   	});
//
//			  JavaRDD<Article> articleRdd =  jrdd.flatMap(new FlatMapFunction<Row, Article> (){
//
//				public Iterable<Article> call(Row t) throws Exception {
//					String content = t.getString(0);
//					List<Article> filter = new ArrayList<Article>();
//					try{
//						JSONObject json = JSON.parseObject(content);
//						if(json.containsKey("type")){// please refer to "大数据平台数据接口.docx->1.2 数据格式"
//
//
//							String type = json.getString("type");
//
//							JSONArray data = json.getJSONArray("data");
//
//
//							List<Article> list = JSON.parseArray(data.toJSONString(), Article.class);
//
//							for(Article article:list){
//								article.setDownload_type(type);
//								int downloadType = Integer.parseInt(type);
//
//								if(downloadType<200&&downloadType>99){//downloadType 请参考 “数据平台交接文档（潘建平）.docx”中“downloadType描述”部分
//									article.setArticle_type(1);
//								}else if(downloadType<300&&downloadType>199){
//									article.setArticle_type(2);
//									article.setPic(null);
//								}else if(downloadType==-1){
//									article.setArticle_type(-1);//from k3,will replaced by certain type later
//									if(article.getMe()==6){
//										if(null==article.getCrawler_site_id()||!"1".endsWith(article.getCrawler_site_id().trim())){//0:新浪微博，1：腾讯微博
//											article.setCrawler_site_id("0");//新浪微博
//										}
//										article.setArticle_type(0);
//									}else{
//										article.setCrawler_site_id(Integer.parseInt(article.getCrawler_site_id())+1000000+"");//k3 传过来的网站ID都在K3的基础上加1000000以避免与新的采集系统传过来的网站ID冲突
//
//									}
//
//								}else if(downloadType>0&&downloadType<99){
//									if(null==article.getCrawler_site_id()||!"1".endsWith(article.getCrawler_site_id().trim())){
//										article.setCrawler_site_id("0");//新浪微博
//									}
//									article.setArticle_type(0);
//								}
//								if(null!=article.getArticle_type()&&null!=article.getCrawler_site_id()&&null!=article.getMid()&&null!=article.getCreated_at()&&null!=article.getUrl()){//数据过滤
//
//									       final long interval = 1000*3600*24L*5;//5 days
//
//											long now = Calendar.getInstance().getTimeInMillis();
//
//											long createdTime =article.getCreated_at().getTime();
//
////											if(createdTime>now-1000*3600){
////												System.out.println("###created_at before:"+es_weibo.getCreated_at()+","+es_weibo.getMid()+",type:"+es_weibo.getDownload_type());
////											}
//											String force = article.getForce();
//
//											if(null!=force&&"1".equals(force)){
//												if(now+1000*60*5+1000*60*60l*48>createdTime){//K3的数据发布日期可能大于今天
//													filter.add(article);
//												}
//											}else if(((now-createdTime<interval)&&(now+1000*60*5+1000*60*60l*48>createdTime))){
//												filter.add(article);
//											}
//
//								}
//
//							}
//
//						}
//					}catch(Exception e){
//						e.printStackTrace();
//						logger.error(e.getMessage(), e);
//						logger.warn("invlid json");
//					}
//
//
//					return filter;
//				}
//
//			  });
//
//
//			  JavaPairRDD<String, Article> jp= articleRdd.mapToPair(new PairFunction<Article, String, Article>() {
//
//					public Tuple2<String, Article> call(Article t) throws Exception {
//						return new Tuple2<String, Article>(t.getMid(), t);
//					}
//
//				});
//			  articleRdd=jp.reduceByKey(new Function2<Article, Article, Article>(){//根据mid和create_at去重
//
//					public Article call(Article v1, Article v2) throws Exception {
//						if(null!=v1.getCreated_at()&&null!=v2.getCreated_at()&&v1.getCreated_at().after(v2.getCreated_at())){
//							return v1;
//						}
//						return v2;
//					}
//
//				  }).map(new Function<Tuple2<String, Article>, Article>(){
//
//					public Article call(Tuple2<String, Article> v) throws Exception {//将数据转化为AMS所需的数据格式
//
//						Article article = v._2;
//						article.setSourceMid(article.getCrawler_site_id()+"_"+article.getMid());
//						article.setMid(article.getCrawler_site_id()+"_"+article.getMid());
//						if(article.getArticle_type()==2&&null!=article.getBiz()&&!article.getBiz().trim().isEmpty()){
//							article.setUid(article.getBiz());
//						}
//						if(null!=article.getUid()){
//							article.setUid(article.getCrawler_site_id()+"_"+article.getUid());
//						}
//
//
//						article.setWeibo_url(article.getUrl());
//						if(null!=article.getCreated_at()){
//							article.setCreated_date(df.format(article.getCreated_at()));
//						}
//						if(null!=article.getCrawler_time()){
//							Calendar c = Calendar.getInstance();
//							c.setTimeInMillis(Long.parseLong(article.getCrawler_time()));
//							article.setCrawler_time(df2.format(c.getTime()));
//						}
//						if(article.getArticle_type()==-1){//根据k3数据中的me字段转化为ams的article_type
//							int me = article.getMe();
//							if(me==1){
//								article.setArticle_type(1);
//							}else if(me==2){
//								article.setArticle_type(3);
//							}else if(me==3){
//								article.setArticle_type(4);
//							}else if(me==4){
//								article.setArticle_type(5);
//							}else if(me==5){
//								article.setArticle_type(6);
//							}else if(me==6){
//								article.setArticle_type(0);
//							}else if(me==7){
//								article.setArticle_type(2);
//							}else if(me==8){
//								article.setArticle_type(7);
//							}else if(me==9){
//								article.setArticle_type(1);
//							}else if(me==21){
//								article.setArticle_type(8);
//							}else if(me==11){
//								article.setArticle_type(10);
//							}else if(me==-1){
//								article.setArticle_type(9);
//							}
//						}
//						changeArticleMid(article);
//						//logger.info(article.getMid());
//						System.out.println(article.getMid());
//						return article;
//
//					}
//
//				  });
//
//			  List<Article> selfDeduplicationArticles = articleRdd.collect();
//
//
//			  List<Article> articles = new ArrayList<Article>();
//			  List<Article> weibos = new ArrayList<Article>();
//			  List<ArticleModifier> weiboModifiers = new ArrayList<ArticleModifier>();
//			  List<ArticleModifier> articleModifiers = new ArrayList<ArticleModifier>();
//			  for(Article article:selfDeduplicationArticles){
//				  boolean exists = false;
//				  if("250".equals(article.getDownload_type())){//微信公众号历史数据
//					  article.setHistory("1");
//				  }else{
//					  exists=JedisClusterUtils.isExists(article.getSourceMid(), "");
//				  }
//				  String force = article.getForce();
//
//				  if(!exists||null!=article.getHistory()||(null!=force&&"1".equals(force))){
//					  if(article.getArticle_type()==0){
//						  weibos.add(article);
//					  }else{
//						  articles.add(article);
//					  }
//
//				  }else{
//					  Integer readCount = article.getRead_count();
//					  Integer zanCount = article.getZan_count();
//					  Integer repostsCount= article.getReposts_count();
//					  Integer commentsCount = article.getComments_count();
//					  int articleType = article.getArticle_type();
//					  readCount=readCount==null?0:readCount;
//					  zanCount=zanCount==null?0:zanCount;
//					  repostsCount=repostsCount==null?0:repostsCount;
//					  commentsCount=commentsCount==null?0:commentsCount;
//
//					  if((readCount+zanCount+repostsCount+commentsCount>40)||(null!=article.getHistory()&&"1".equals(article.getHistory()))){
//						  ArticleModifier modifier = new ArticleModifier();
//						  modifier.setArticle_type(articleType);
//						  modifier.setComments_count(commentsCount);
//						  modifier.setRead_count(readCount);
//						  modifier.setReposts_count(repostsCount);
//						  modifier.setComments_count(commentsCount);
//						  modifier.setCrawler_time(article.getCrawler_time());
//						  modifier.setMid(article.getMid());
//						  if("250".equals(article.getDownload_type())){//微信公众号评论转发历史数据S
//							  modifier.setHistory("1");
//						  }
//						  if(articleType==0){
//							  weiboModifiers.add(modifier);
//						  }else{
//							  articleModifiers.add(modifier);
//						  }
//					  }
//
//
//
//				  }
//			  }
//
//
//			  System.out.println("self-deduplication article size:"+selfDeduplicationArticles.size()+",after redis-deduplication article size:"+articles.size()+",weibo size"+weibos.size());
//			  int size = articles.size();
//
//			  int sendTimes = size%maxSendSize==0?size/maxSendSize:size/maxSendSize+1;
//
//			  //新增（非微博）文章
//			  for(int i=0;i<sendTimes;i++){
//				  int fromIndex = i*maxSendSize;
//				  int toIndex=(i+1)*maxSendSize>size?size:(i+1)*maxSendSize;
//				  List<Article> subArticles = articles.subList(fromIndex, toIndex);
//				  String jsonArray = JSON.toJSON(subArticles).toString();
//				   String articleDatas = "{\"type\":\"article\",\"data\":"+jsonArray+"}";
//					long  start= Calendar.getInstance().getTimeInMillis();
//					Article2KafkaDetectTopic.sendNews(articleDatas);
//					long  end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to article_detect_data,type=article,size="+subArticles.size()+",cost:"+(end-start)+"ms");
//			  }
//
//
//			  size = weibos.size();
//
//			  sendTimes = size%maxSendSize==0?size/maxSendSize:size/maxSendSize+1;
//			  //新增微博文章
//			  for(int i=0;i<sendTimes;i++){
//				  int fromIndex = i*maxSendSize;
//				  int toIndex=(i+1)*maxSendSize>size?size:(i+1)*maxSendSize;
//				  List<Article> subArticles = weibos.subList(fromIndex, toIndex);
//				  String jsonArray = JSON.toJSON(subArticles).toString();
//				   String articleDatas = "{\"type\":\"weibo\",\"data\":"+jsonArray+"}";
//					long  start= Calendar.getInstance().getTimeInMillis();
//					Article2KafkaDetectTopic.sendNews(articleDatas);
//					long  end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to article_detect_data,type=weibo,size="+subArticles.size()+",cost:"+(end-start)+"ms");
//			  }
//
//			  //需计算汉明码的（非微博）文章
//			 for(Article article:articles){
//				 long start= Calendar.getInstance().getTimeInMillis();
//			    	String news = article.getMid() + "|"  + article.getText();
//
//			    	//计算汉明码
//			    	Article2KafkaHanmingCodeTopic.sendNews(news);
//					long end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to hanming_data,type=article,size=1,cost:"+(end-start)+"ms");
//			 }
//
//			//需计算汉明码的微博文章
//			 for(Article article:weibos){
//				 long start= Calendar.getInstance().getTimeInMillis();
//			    	String news = article.getMid() + "|"  + article.getText();
//
//			    	//计算汉明码
//			    	Article2KafkaHanmingCodeTopic.sendNews(news);
//					long end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to hanming_data,type=weibo,size=1,cost:"+(end-start)+"ms");
//			 }
//
//			//需更新的微博文章
//			  size = weiboModifiers.size();
//			  sendTimes = size%maxSendSize==0?size/maxSendSize:size/maxSendSize+1;
//			  for(int i=0;i<sendTimes;i++){
//				  int fromIndex = i*maxSendSize;
//				  int toIndex=(i+1)*maxSendSize>size?size:(i+1)*maxSendSize;
//				  List<ArticleModifier> subArticles = weiboModifiers.subList(fromIndex, toIndex);
//				  String jsonArray = JSON.toJSON(subArticles).toString();
//				   String articleDatas = "{\"type\":\"weibo\",\"data\":"+jsonArray+"}";
//					long  start= Calendar.getInstance().getTimeInMillis();
//					ArticleModifier2KafkaTopic.sendNews(articleDatas);
//					long  end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to update_data,type=weibo,size="+subArticles.size()+",cost:"+(end-start)+"ms");
//			  }
//
//			//需更新的（非微博）文章
//			  size = articleModifiers.size();
//			  sendTimes = size%maxSendSize==0?size/maxSendSize:size/maxSendSize+1;
//			  for(int i=0;i<sendTimes;i++){
//				  int fromIndex = i*maxSendSize;
//				  int toIndex=(i+1)*maxSendSize>size?size:(i+1)*maxSendSize;
//				  List<ArticleModifier> subArticles = articleModifiers.subList(fromIndex, toIndex);
//				  String jsonArray = JSON.toJSON(subArticles).toString();
//				   String articleDatas = "{\"type\":\"article\",\"data\":"+jsonArray+"}";
//					long  start= Calendar.getInstance().getTimeInMillis();
//					ArticleModifier2KafkaTopic.sendNews(articleDatas);
//					long  end= Calendar.getInstance().getTimeInMillis();
//					System.out.println(Thread.currentThread()+"###send to update_data,type=article,size="+subArticles.size()+",cost:"+(end-start)+"ms");
//			  }
//
//
//
//
//
//	        Map<Integer,Long> partitionOffsets = new HashMap<Integer,Long>();
//	        for(OffsetRange offsetRange:offsets){
//	        	long untilOffset = offsetRange.untilOffset();
//	        	int partition = offsetRange.partition();
//	        	partitionOffsets.put(partition, untilOffset);
//
//	        }
//	        ZKUtils.writeOffsetToZookeeper(group, offsets[0].topic(), partitionOffsets);
//	        partitionOffsets = ZKUtils.getPartitionOffset(group, offsets[0].topic());
//	        for(Integer partition:partitionOffsets.keySet()){
//	        	System.out.println(partition+":"+partitionOffsets.get(partition));
//	        }
//
//
//			return null;
//		}
//
//    });
//
//
//    System.out.println("finished..");
//
//    //stream.print();
//
//    jssc.start();
//
//    jssc.awaitTermination();
//  }
//
//
//  public static void changeArticleMid(Article article) {
//		String mid = article.getMid();
//		String uid = article.getUid();
//		if(article.getArticle_type()==0){
//			article.setMid(MD5(mid) +getData(article.getCreated_at())+mid);
//		}else{
//			article.setMid(MD5(mid) + mid);
//		}
//
//		if(null!=uid&&!uid.trim().isEmpty()){
//			article.setUid(MD5(uid) + uid);
//		}
//
//	}
//
//
//	private static String MD5(String str) {
//		if(str==null){
//			str ="";
//		}
//
//		try {
//			return Md5Utils.getMd5ByStr(str).substring(0, 4);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return str;
//		}
//	}
//
//	// public static void main(String[] args) {
//	// ChangeMidUtils c = new ChangeMidUtils();
//	// String md5 = c.MD5("0_3948867035028915");
//	// System.out.println(md5);
//	//
//	// }
//
//	private static String getData(Timestamp created_at) {
//		if(created_at==null){
//			return "";
//		}
//		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
//		String timeStr = df.format(created_at);
//		return timeStr;
//	}
//
//
//
//}
