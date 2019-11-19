package com.cloudera.vms.jobs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.vms.bean.Article;
import com.cloudera.vms.bean.ArticleHanmingCodeBean;
import com.cloudera.vms.bean.ArticleModifier;
import com.cloudera.vms.logs.ArticleModifier2KafkaTopic;
import com.cloudera.vms.logs.LinkLogUtils;
import com.cloudera.vms.utils.*;
import com.izhonghong.vms.zookeeper.ZKUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.junit.Test;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

//import org.slf4j.Logger;

//import org.apache.spark.streaming.kafka010.KafkaUtils;

//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.apache.spark.streaming.kafka010.OffsetRange;
//import org.apache.spark.streaming.kafka010.HasOffsetRanges;

/**
 * Consumes messages from topic in Kafka and distribute the articles to other topics
 * <p>
 * Usage: ArticleDistributionJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>
 * <zkQuorums> is a list of one or more zookeeper servers that make quorum
 * <group> is the name of kafka consumer group
 * <topic> is a  kafka topic to consume from
 * <totalPartitions> total partitions of the topic
 * <numThreads> is the number of threads the kafka consumer should use
 * <duration>   The time interval at which streaming data will be divided into batches
 * To run this example:
 * sudo ./spark-submit  --class HanmingCodeComputeJob  --master spark://test11:7077  --executor-memory 4G --total-executor-cores 10 --jars (jars separated by ",") /home/izhonghong/test/spark-test/spark-test-0.0.1-SNAPSHOT.jar test11:2181,test12:2181,test13:2181 g3 weibo-simple2 4 4 5000
 */

public class ArticleDistributionJob {

    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger logger = LogManager.getLogger(ArticleDistributionJob.class);

    //private static final Logger logger = LoggerFactory.getLogger(ArticleDistributionJob.class);
    private static final long interval5 = 1000 * 3600 * 24L * 5;//5 days
    private static final long interval3 = 1000 * 3600 * 24L * 3;//3 days
    private static final long intervalYun = 1000 * 3600 * 24l * 90;//90 元搜索90天
    private static final long interval_top = 1000 * 60 * 5 + 1000 * 60 * 60l * 48;//2 days + 5分钟
    static int expireTime = 60 * 60 * 24 * 7;//7天
    static int sourceExpireTime = 60 * 60 * 24 * 90;//90天
    static Long starttime = 1514736000000L;
    //private static DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static DateFormat df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static int maxSendSize = 50;
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public ArticleDistributionJob() {
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: ArticleDistributionJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
            args = new String[]{"test11:9092,test12:9092,test13:9092", "article", "exclude_weibo_data", "1", "4", "2000"};
//            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("ArticleDistributionJob");

        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
       /* sparkConf.set("spark.default.parallelism","200");
        sparkConf.set("spark.storage.memoryFraction","0.2");
        sparkConf.set("spark.shuffle.memoryFraction","0.6");
        sparkConf.set("spark.serialize","org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max","2000");
        sparkConf.set("spark.driver.maxResultSize","0.2");
        sparkConf.set("spark.storage.memoryFraction","0.2");*/

//        sparkConf.set("spark.streaming.unpersist", "true");
//        sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC");

        //kafka集群列表
        String brokerList = args[0];
        //消费组
        final String group = args[1];
        //消费topic
        String topic = args[2];
        //partition数量
        int totalPartitions = Integer.parseInt(args[3]);
        //
        final int numThreads = Integer.parseInt(args[4]);
        int duration = Integer.parseInt(args[5]);

        // Create the context with 2 seconds batch size
        //sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
        //jssc.sc().addJar("hdfs:///app/hduser1301/lib/hbase-client-1.2.2.jar");
        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        topicMap.put(topic, numThreads);


        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokerList);


        kafkaParams.put("group.id", group);

        kafkaParams.put("session.timeout.ms", "30000");

        kafkaParams.put("request.timeout.ms", "60000");
        //kafkaParams.put("fetch.message.max.bytes", "5000000");//打开最大消息限制
        //kafkaParams.put("num.replica.fetchers","3");


        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
        Map<Integer, Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);

        System.out.println("partitionoffsets:" + partitionOffsets);//zookeeper
        Map<Integer, Long> earlistOffsets = null;
        try {
            //注意有上海和华为云的区别
            earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
        } catch (Exception e) {
            e.printStackTrace();
            //logger.error("failed to get the partitions' offset, the topic:" + topic + " may doesn't exist");
        }

        if (null != partitionOffsets && partitionOffsets.size() > 0) {
            for (Integer partition : partitionOffsets.keySet()) {
                try {
                    long offset = partitionOffsets.get(partition);//zk

                    long earlistOffset = earlistOffsets.get(partition);//kafka中的

                    logger.info("partition:" + partition + ",offset:" + offset + ",earlistOffset:" + earlistOffset);
                    if (earlistOffset > offset) {
                        offset = earlistOffset;
                    }
                    fromOffsets.put(new TopicAndPartition(topic, partition), offset);
                    if (fromOffsets.size() < totalPartitions) {
                        for (int i = fromOffsets.size(); i < totalPartitions; i++) {
                            fromOffsets.put(new TopicAndPartition(topic, i), 0L);
                        }
                    }
                } catch (Exception e) {
                    logger.error("error:" + e);
                }
            }
        } else {
            for (int i = 0; i < totalPartitions; i++) {
                fromOffsets.put(new TopicAndPartition(topic, i), earlistOffsets.get(i));
            }
        }

        for (TopicAndPartition partition : fromOffsets.keySet()) {
            System.out.println(topic + ":" + partition + ":" + fromOffsets.get(partition));
        }
        logger.info("kafkaParams:" + kafkaParams);


        //create the direct stream, with witch we can maintain the offset in zookeeper by ourselves

        JavaInputDStream<Row> stream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                StringDecoder.class, Row.class, kafkaParams, fromOffsets,
                new Function<MessageAndMetadata<String, String>, Row>() {
                    public Row call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        String message = v1.message();

                        return RowFactory.create(message);
                    }
                });
        //final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jssc.sc());


        logger.info("=========================lines count:" + stream.count());



//        Function<JavaRDD<Row>, Void> foreachFunc = ;

        stream.foreachRDD((Function<JavaRDD<Row>, Void>) jrdd -> {
            logger.info("#### start");
            Calendar instance = Calendar.getInstance();
            instance.add(Calendar.MONTH, -3);
            final Date date90 = instance.getTime();

            OffsetRange[] offsets = ((HasOffsetRanges) jrdd.rdd()).offsetRanges();

            jrdd = jrdd.filter(new Function<Row, Boolean>() {//去除空的message

                public Boolean call(Row v1) throws Exception {
                    //logger.info("article :"+v1.get(0));
                    return v1 != null && v1.get(0) != null;
                }
            });

            JavaRDD<Article> articleRdd = jrdd.flatMap(new FlatMapFunction<Row, Article>() {

                public Iterable<Article> call(Row t) throws Exception {
                    String content = t.getString(0);
                    List<Article> filter = new ArrayList<Article>();
                    try {
                        JSONObject json = JSON.parseObject(content);
                        if (json.containsKey("type")) {// please refer to "大数据平台数据接口.docx->1.2 数据格式"
                            String type = json.getString("type");
                            JSONArray data = json.getJSONArray("data");

                            List<Article> list = JSON.parseArray(data.toJSONString(), Article.class);//解析数组数据
                            for (Article article : list) {
                                //logger.info("url:"+article.getMid());//打印rul
                                article.setDownload_type(type);
                                int downloadType = Integer.parseInt(type);
                                // System.out.println("33333");
                                if (downloadType < 200 && downloadType > 99 && downloadType != 110) {//downloadType 请参考 “数据平台交接文档（潘建平）.docx”中“downloadType描述”部分
                                    if (null == article.getArticle_type() || 0 == article.getArticle_type()) {
                                        article.setArticle_type(11);//APP
                                    }
                                } else if (downloadType == 110 || (downloadType > 300 && downloadType < 320)) {
                                    if (article.getCategoryId() != null) {
                                        article.setArticle_type(article.getCategoryId());
                                    }
                                } else if (downloadType < 300 && downloadType > 199) {
                                    article.setArticle_type(2);
                                    article.setPic(null);
                                } else if (downloadType == -1) {
                                    if (null == article.getArticle_type()) {//网研主贴 、回帖数据为article_type=8
                                        article.setArticle_type(-1);//from k3,will replaced by certain type later
                                    }
                                    if (article.getMe() == 6) {
                                        if (null == article.getCrawler_site_id() || !"1".endsWith(article.getCrawler_site_id().trim())) {//0:新浪微博，1：腾讯微博
                                            article.setCrawler_site_id("0");//新浪微博 else腾讯微博
                                        }
                                        article.setArticle_type(0);
                                    } else {
                                        if (null == article.getTags()) {//非null的目前为网研数据,网研数据不需要走该流程
                                            article.setCrawler_site_id(Integer.parseInt(article.getCrawler_site_id()) + 1000000 + "");//k3 传过来的网站ID都在K3的基础上加1000000以避免与新的采集系统传过来的网站ID冲突
                                        }
                                    }

                                } else if (downloadType > 0 && downloadType < 99) {
                                    //logger.info("url:" + article.getMid());//打印rul
                                    //新浪微博0~79；舆情通80~89；腾讯微博90~99，
//                                        if (null == article.getCrawler_site_id() || !"1".endsWith(article.getCrawler_site_id().trim())) {
                                    if (null == article.getCrawler_site_id()) {
                                        article.setCrawler_site_id("0");//新浪微博
                                    } else {
                                        logger.info(article.getCrawler_site_id());//todo 腾讯微博
                                    }
                                    article.setArticle_type(0);
                                } else {
                                    //article.setArticle_type(1001);
                                    logger.info("downloadType异常：" + downloadType + ",mid:" + article.getMid() + ":" + "crawler_siteid:" + article.getCrawler_site_id() +
                                            "create_at:" + article.getCreated_at());
                                }
                                ////数据过滤(针对日期）
                                if ((null != article.getForce()) || (null != article.getArticle_type() && null != article.getCrawler_site_id()
                                        && null != article.getMid() && null != article.getCreated_at()
                                        && null != article.getUrl())) {

                                    long now = Calendar.getInstance().getTimeInMillis();

                                    long createdTime = article.getCreated_at().getTime();

//											if(createdTime>now-1000*3600){
//												logger.info("###created_at before:"+es_weibo.getCreated_at()+","+es_weibo.getMid()+",type:"+es_weibo.getDownload_type());
//											}
                                    String force = article.getForce();

//                                        if (null != force && "1".equals(force){
                                    if (null != article.getEvents_tag() || null != force) {
                                        // 特殊数据，强制插入
                                        if (now + interval_top > createdTime) {//K3的数据发布日期可能大于今天 <
                                            if (new Date(createdTime).after(date90)) {
                                                filter.add(article);
                                            }
                                        }
                                    } else {
                                        long l = now - createdTime;
                                        //元搜索数据期限为90天
                                        if (downloadType == 110 && (l < intervalYun) && ((now + interval_top > createdTime))) {
                                            filter.add(article);

                                        } else {
                                            if (((l < interval5) && (now + interval_top > createdTime))) {
                                                /*if (article.getArticle_type() == 0) {
                                                    //微博数据只保留三天内的数据
                                                    if (l < interval3) {
                                                        filter.add(article);
                                                    }
                                                } else {
                                                    filter.add(article);
                                                }*/
                                                filter.add(article);
                                            } else {
                                                //logger.info("发表时间不符合范围，过滤掉：" + article.getMid());
                                                //                                            logger.info("createdTime out,filter：" + article.getMid());
                                            }

                                        }
                                    }
                                }
                            }

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error(e.getMessage());
                        //logger.warn("###invlid json！！！" + content.substring(0, 32));
                        logger.error("###invlid json！！！" + content);
                    }
                    return filter;
                }

            });


            JavaPairRDD<String, Article> jp = articleRdd.mapToPair(new PairFunction<Article, String, Article>() {

                public Tuple2<String, Article> call(Article t) throws Exception {
                    return new Tuple2<String, Article>(t.getMid(), t);
                }

            });
            articleRdd = jp.reduceByKey(new Function2<Article, Article, Article>() {//根据mid和create_at去重

                public Article call(Article v1, Article v2) throws Exception {
                    if (null != v1.getCreated_at() && null != v2.getCreated_at() && v1.getCreated_at().after(v2.getCreated_at())) {
                        return v1;
                    } else {
                        return v2;
                    }
                }

            }).map(new Function<Tuple2<String, Article>, Article>() {//将数据转化为AMS所需的数据格式

                public Article call(Tuple2<String, Article> v) throws Exception {
                    DateFormat df = new SimpleDateFormat("yyyyMMdd");
                    Article article = v._2;
                    String downloadType = article.getDownload_type();
                    String force = article.getForce();
                    article.setSourceMid(article.getCrawler_site_id() + "_" + article.getMid());//网站id_mid 去重点

                    if (null != downloadType) {
                        if (!"2".equals(force)) {//2类型不进行加密
                            //if (! "1001".equals(downloadType)) {//1001类型单独加密
                            article.setMid(article.getCrawler_site_id() + "_" + article.getMid());
                        }

                    }

                    /*if (article.getArticle_type() == 2 && StringUtils.isNotBlank(article.getBiz())) {//微信
                        article.setUid(article.getBiz());
                    }*/
                    if (StringUtils.isNotBlank(article.getUid()) && 11 != article.getArticle_type()) {
                        article.setUid(article.getCrawler_site_id() + "_" + article.getUid());
                    }

                    //对app、twitter、facebook类uid不用加密
                   /* if (StringUtils.isBlank(article.getUid()) || 11 == article.getArticle_type() || "999999".equals(article.getCrawler_site_id())
                            || "3115345".equals(article.getCrawler_site_id())) {

                    }else {
                        article.setUid(article.getCrawler_site_id() + "_" + article.getUid());
                    }*/

                    article.setWeibo_url(article.getUrl());

                    String mid = article.getMid();
                    //对k3及100的数据再进行加密(主要针对k3的mid不同，text，url相同的数据进行去重）,除去twitter
                    //if ((article.getDownload_type().equals("-1") || article.getDownload_type().equals("100")) && article.getUrl() != null) {
                    if ("999999".equals(article.getCrawler_site_id())) {
                        article.setSourceMid(MD5(article.getUrl()) + article.getMid());
                    } else if (article.getDownload_type().equals("-1") || (11 == article.getArticle_type())) {
                        String url = article.getUrl();
                        boolean flag = url.contains("#33#");
                        if (url.contains("#")) {
                            url = url.substring(0, url.indexOf("#"));
                        }
                        if (StringUtils.isNotBlank(article.getText())) {
                            if (url.contains("?")) {
                                url = url.substring(0, url.indexOf("?"));
                            }
                            url = url + article.getText();
                        }

                        if (flag) {
                            String mid1 = Md5Utils.getMd5ByStr(url + mid);
                            article.setSourceMid(mid1);
                            article.setMid(mid1);
                        } else {
                            String mid2 = Md5Utils.getMd5ByStr(url);
                            article.setSourceMid(mid2);
                            article.setMid(mid2);
                        }
                    }
                    if (StringUtils.isNotBlank(article.getCrawler_time())) {
                        try {
                            Long crawTime = Long.parseLong(article.getCrawler_time());
                            Long newTime = System.currentTimeMillis();
                            Long createAt = article.getCreated_at().getTime();

                           /* Calendar c = Calendar.getInstance();
                            c.setTimeInMillis(crawTime);//爬取时间*/
                            //Date newTime = Calendar.getInstance().getTime();//当前时间
                            if ((crawTime < starttime) || (newTime < crawTime)) {
                                logger.error("出现采集时间异常数据->crawlerTime:" + article.getCrawler_time() + ",mid:" + article.getMid() + ",downloadType:" + article.getDownload_type());
                                article.setCrawler_time(df2.format(newTime));
                                //todo 发表时间大于当前时间，设置发表时间为爬取时间
                                //if (Calendar.getInstance().getTime().before(article.getCreated_at())) {
                                if ((createAt < starttime) || (newTime < createAt)) {
                                    logger.error("出现发布时间异常数据->createTime:" + article.getCreated_at() + ",mid:" + article.getMid() + ",downloadType:" + article.getDownload_type());
                                    article.setCreated_at(new Timestamp(newTime));
                                }
                            } else {
                                article.setCrawler_time(df2.format(crawTime));
                                if ((createAt < starttime) || (newTime < createAt)) {
                                    logger.error("出现发布时间异常数据->createTime:" + article.getCreated_at() + ",mid:" + article.getMid() + ",downloadType:" + article.getDownload_type());
                                    article.setCreated_at(new Timestamp(crawTime));
                                }

                            }
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                            logger.error("转换采集时间错误：" + JSON.toJSONString(article) + e);
                        }
                    } else {
                        //Date newTime = Calendar.getInstance().getTime();//当前时间
                        Long newTime = System.currentTimeMillis();
                        article.setCrawler_time(df2.format(newTime));
                        Long createAt = article.getCreated_at().getTime();
                        if ((createAt < starttime) || (newTime < createAt)) {
                            logger.error("出现发布时间异常数据->createTime:" + article.getCreated_at() + ",mid:" + article.getMid() + ",downloadType:" + article.getDownload_type());
                            article.setCreated_at(new Timestamp(newTime));
                        }

                    }

                    //转换时间格式
                    if (null != article.getCreated_at()) {
                        article.setCreated_date(df.format(article.getCreated_at()));
                        df = null;

                    }
                    if (article.getArticle_type() == -1) {//根据k3数据中的me字段转化为ams的article_type
                        try {
                            String crawlerSiteId = article.getCrawler_site_id();
                            String articleType = JedisClusterUtils.getJedisCluster().hget("domain-media", crawlerSiteId);
                            if (null != articleType) {
                                //logger.info("articleType:" + articleType);
                                article.setArticle_type(Integer.parseInt(articleType));
                            } else if (0 != Integer.valueOf(article.getMe())) {
                                int me = article.getMe();
                                if (me == 1) {
                                    article.setArticle_type(1);
                                } else if (me == 2) {
                                    article.setArticle_type(3);
                                } else if (me == 3) {
                                    article.setArticle_type(4);
                                } else if (me == 4) {
                                    article.setArticle_type(5);
                                } else if (me == 5) {
                                    article.setArticle_type(6);
                                } else if (me == 6) {
                                    article.setArticle_type(0);
                                } else if (me == 7) {
                                    article.setArticle_type(2);
                                } else if (me == 8) {
                                    article.setArticle_type(7);
                                } else if (me == 9) {
                                    article.setArticle_type(11);//APP数据
                                } else if (me == 21) {
                                    article.setArticle_type(8);
                                } else if (me == 11) {
                                    article.setArticle_type(10);
                                    article.setDownload_type("301");//Twitter,帐号采集
                                } else if (me == 12) {
                                    article.setArticle_type(10);
                                    article.setDownload_type("305");//Twitter,元搜索
                                } else if (me == -1) {
                                    article.setArticle_type(9);
                                }
                                //logger.info("articleType is null !");
                            } else if (3 != article.getArticle_type() && null != article.getCategoryId()) {//3的是网研论坛主贴的
                                article.setArticle_type(article.getCategoryId());
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("k3 error:" + article.getMid() + e);
                        }

                    }
                    changeArticleMid(article);

                    return article;

                }
            });

            logger.info("###flag");
            List<Article> selfDeduplicationArticles = articleRdd.collect();//todo 有毒
            int collectSize = selfDeduplicationArticles.size();
            logger.info("###flag " + collectSize);


            List<Article> articles = new CopyOnWriteArrayList<Article>();
            List<Article> weibos = new CopyOnWriteArrayList<Article>();
            List<ArticleModifier> weiboModifiers = new CopyOnWriteArrayList<ArticleModifier>();
            List<ArticleModifier> articleModifiers = new CopyOnWriteArrayList<ArticleModifier>();

            int redis_batch_num = 1000;
            int redis_thread = collectSize % redis_batch_num == 0 ? collectSize / redis_batch_num : collectSize / redis_batch_num + 1;
            CountDownLatch countDownLatch = new CountDownLatch(redis_thread);
            logger.info("###flag " + collectSize + " redis_thread:" + redis_thread);
            for (int i = 0; i < redis_thread; i++) {
                int fromIndex = i * redis_batch_num;
                int toIndex = (i + 1) * redis_batch_num > collectSize ? collectSize : (i + 1) * redis_batch_num;
                List<Article> subList = selfDeduplicationArticles.subList(fromIndex, toIndex);
                executorService.execute(new Thread(() -> {
                    for (Article article : subList) {
                        LinkLogUtils ut = new LinkLogUtils();
                        int articleType = article.getArticle_type();
                        Integer zansCount = article.getZan_count();
                        Integer reports_count = article.getReposts_count();
                        Integer readCount = article.getRead_count();

                        String verifiedtype = article.getVerified_type();

                        Integer commentsCount = article.getComments_count();


                        article.setComments_count(commentsCount);
                        article.setVerifiedtype(verifiedtype);
                        article.setReports_count(reports_count);
                        article.setZans_count(zansCount);
                        article.setRead_count(readCount);

                        String force = article.getForce();
                        String downType = article.getDownload_type();
                        int downloadType = Integer.parseInt(downType);
                        List<String> tag = article.getTags();
                        boolean isWy = false;
                        if (null != tag && tag.contains("dataSource:1")) {
                            isWy = true;
                        }

                        boolean exists = false;
                        if (6 == articleType) {//针对视频类正文为空的赋值为title
                            String title = article.getTitle();
                            String text = article.getText();
                            if ((StringUtils.isNotBlank(title)) && (StringUtils.isBlank(text))) {
                                article.setText(title);
                            }
                        }
                        if ("250".equals(article.getDownload_type())) {
                            article.setHistory("1");
                        }
                        //"1".equals(force) ||  "2".equals(force)  //"1".equals(force) ||  "2".equals(force)
                        if (null != article.getForce() || null != article.getHistory()) {//微信公众号历史数据
                            exists = false;//force = 1 强制更新
                        } else {
                            if (StringUtils.isNotBlank(article.getText())) {//第一次解析text为空，而后text不为空，然而去重掉造成数据丢失
                                //方法里面有上海和华为云集群之别 302
                                if (110 == downloadType || 302 == downloadType || 303 == downloadType || 304 == downloadType || 305 == downloadType) {//境外采集
                                    exists = JedisClusterUtils.isExists(article.getSourceMid(), "", sourceExpireTime);//todo 性能瓶颈！
                                } else {//文章
                                    exists = JedisClusterUtils.isExists(article.getSourceMid(), "", expireTime);//
                                }
                                //针对iget、网研、原搜索重复数据进行二次去重(针对境外数据和网研数据）
                                if (((9 == articleType) || isWy) && (3 != articleType) && (8 != articleType) && (!exists)) {
                                    exists = JedisClusterUtils.isExists(Md5Utils.getMd5ByStr(article.getUrl() + "cf"), "", expireTime);
                                }
                            }
                        }


                        if (null != article.getEvents_tag() || (!exists)) {//有events_tag的强制进入数据流

                            if (article.getArticle_type() == 0) {
                                weibos.add(article);
                            } else {
                                articles.add(article);
                            }
                            ut.getLog(article.getMid(), "1");

                        } else {//如果存在
                            String url = article.getUrl();

                            readCount = readCount == null ? 0 : readCount;
                            reports_count = reports_count == null ? 0 : reports_count;
                            zansCount = zansCount == null ? 0 : zansCount;
                            commentsCount = commentsCount == null ? 0 : commentsCount;

                            ArticleModifier modifier = new ArticleModifier();
                            modifier.setMid(article.getMid());

                            modifier.setHistory(article.getHistory());
                            modifier.setCrawler_time(article.getCrawler_time());
                            modifier.setArticle_type(articleType);//
                            modifier.setCreated_at(article.getCreated_at());//文章发布时间,用于入库到哪个索引
                            //订阅id
                            boolean flag = false;
                            if (url.contains("biz")) {
                                flag = true;
                                modifier.setUrl(url);
                            }
                           /* if (article.getTags() != null && article.getTags().size() > 0) {
                                //logger.info("tags:"+article.getMid()+";"+article.getTags());
                                modifier.setTags(article.getTags());
                                flag = true;
                            }*/
                            if ((readCount + zansCount + reports_count + commentsCount > 40) || ("1".equals(article.getHistory()))) {
                                //{
                                modifier.setZans_count(zansCount);
                                modifier.setComments_count(commentsCount);
                                modifier.setRead_count(readCount);
                                modifier.setReports_count(reports_count);
                                flag = true;
                            }
                            if (flag) {
                                if (articleType == 0) {
                                    //logger.info("mid1:" + article.getMid());
                                    weiboModifiers.add(modifier);
                                } else {
                                    articleModifiers.add(modifier);
                                }
                            }
                            ut.getLog(article.getMid(), "2");
                        }

                    }
                    countDownLatch.countDown();
                }));
            }
            countDownLatch.await(2, TimeUnit.MINUTES);

            logger.info("###flag " + collectSize);

            logger.info("self-deduplication article size:" + collectSize + ",after redis-deduplication article size:" + articles.size() + ",weibo size" + weibos.size());

            sendArticleNews(articles);


            sendWeiboNews(weibos);

            //需计算汉明码的（非微博）文章
            //sendToComputeHM(articles);

            //需计算汉明码的微博
            //sendToComputeHM(weibos);

            //需更新的微博文章
            sendModifiedWeiboNews(weiboModifiers);

            //需更新的（非微博）文章
            sendModifiedArticleNews(articleModifiers);


            Map<Integer, Long> partitionOffsets1 = new HashMap<Integer, Long>();
            for (OffsetRange offsetRange : offsets) {
                long untilOffset = offsetRange.untilOffset();
                int partition = offsetRange.partition();
                partitionOffsets1.put(partition, untilOffset);

            }
            ZKUtils.writeOffsetToZookeeper(group, offsets[0].topic(), partitionOffsets1);
            partitionOffsets1 = ZKUtils.getPartitionOffset(group, offsets[0].topic());
            for (Integer partition : partitionOffsets1.keySet()) {
                logger.info(partition + ">>>>>>>>>>>>>>>>:" + partitionOffsets1.get(partition));
            }


            logger.info("###flag " + collectSize);
            return null;
        });


        logger.info("finished..");

        //stream.print();

        jssc.start();


        jssc.awaitTermination();
    }

    private static void sendToComputeHM(List<Article> articles) {
        for (Article article : articles) {
            long start = Calendar.getInstance().getTimeInMillis();
//                    String news = article.getMid() + "|" + article.getText();


            ArticleHanmingCodeBean articleHanmingCodeBean = new ArticleHanmingCodeBean();
            articleHanmingCodeBean.setMid(article.getMid());
            articleHanmingCodeBean.setText(article.getText());
            articleHanmingCodeBean.setCreated_at(article.getCreated_at());
            String news = JSON.toJSONString(articleHanmingCodeBean);

            //计算汉明码
            Article2KafkaHanmingCodeTopic.sendNews(news);
            long end = Calendar.getInstance().getTimeInMillis();
//                    logger.info(Thread.currentThread() + "###send to hanming_data,type=article,size=1,cost:" + (end - start) + "ms");
        }
    }

    private static void sendModifiedArticleNews(List<ArticleModifier> articleModifiers) {

        int size = articleModifiers.size();
        int sendTimes = size % maxSendSize == 0 ? size / maxSendSize : size / maxSendSize + 1;

        for (int i = 0; i < sendTimes; i++) {
            int fromIndex = i * maxSendSize;
            int toIndex = (i + 1) * maxSendSize > size ? size : (i + 1) * maxSendSize;
            List<ArticleModifier> subArticles = articleModifiers.subList(fromIndex, toIndex);
            String jsonArray = JSON.toJSON(subArticles).toString();
            String articleDatas = "{\"type\":\"article\",\"data\":" + jsonArray + "}";
            long start = Calendar.getInstance().getTimeInMillis();
            ArticleModifier2KafkaTopic.sendNews(articleDatas);
            long end = Calendar.getInstance().getTimeInMillis();
            logger.info(Thread.currentThread() + "###send to update_data,type=article,size=" + subArticles.size() + ",cost:" + (end - start) + "ms");
        }
    }

    private static void sendModifiedWeiboNews(List<ArticleModifier> weiboModifiers) {
        int size;
        int sendTimes;
        size = weiboModifiers.size();
        sendTimes = size % maxSendSize == 0 ? size / maxSendSize : size / maxSendSize + 1;
        for (int i = 0; i < sendTimes; i++) {
            int fromIndex = i * maxSendSize;
            int toIndex = (i + 1) * maxSendSize > size ? size : (i + 1) * maxSendSize;
            List<ArticleModifier> subArticles = weiboModifiers.subList(fromIndex, toIndex);
            String jsonArray = JSON.toJSON(subArticles).toString();
            String articleDatas = "{\"type\":\"weibo\",\"data\":" + jsonArray + "}";
            long start = Calendar.getInstance().getTimeInMillis();
            ArticleModifier2KafkaTopic.sendNews(articleDatas);
            long end = Calendar.getInstance().getTimeInMillis();
            logger.info(Thread.currentThread() + "###send to update_data,type=weibo,size=" + subArticles.size() + ",cost:" + (end - start) + "ms");
        }
    }

    private static void sendArticleNews(List<Article> articles) {
        int size = articles.size();
        if (size == 0) {
            return;
        }
        Integer downloadType = Integer.valueOf(articles.get(0).getDownload_type());
        int sendTimes = size % maxSendSize == 0 ? size / maxSendSize : size / maxSendSize + 1;
        //新增（非微博）文章
        for (int i = 0; i < sendTimes; i++) {
            int fromIndex = i * maxSendSize;
            int toIndex = (i + 1) * maxSendSize > size ? size : (i + 1) * maxSendSize;
            List<Article> subArticles = articles.subList(fromIndex, toIndex);
            String jsonArray = JSON.toJSON(subArticles).toString();
            String articleDatas = "{\"type\":\"article\",\"data\":" + jsonArray + "}";
            long start = Calendar.getInstance().getTimeInMillis();
            if (downloadType == 110 || downloadType == 226 || downloadType == 102 || (downloadType > 300 && downloadType < 310)) {
                //发送到快速通道的数据
                Article2KafkaDetectTopic.sendNews_priority1(articleDatas);
            } else {
                Article2KafkaDetectTopic.sendNews(articleDatas);
            }
            long end = Calendar.getInstance().getTimeInMillis();
            logger.info(Thread.currentThread() + "###send to article_detect_data,type=article,size=" + subArticles.size() + ",cost:" + (end - start) + "ms");
        }
    }

    private static void sendWeiboNews(List<Article> weibos) {


        int size = weibos.size();
        if (size == 0) {
            return;
        }
        Integer downloadType = Integer.valueOf(weibos.get(0).getDownload_type());
        int sendTimes = size % maxSendSize == 0 ? size / maxSendSize : size / maxSendSize + 1;
        //新增微博文章
        for (int i = 0; i < sendTimes; i++) {
            int fromIndex = i * maxSendSize;
            int toIndex = (i + 1) * maxSendSize > size ? size : (i + 1) * maxSendSize;
            List<Article> subWeibos = weibos.subList(fromIndex, toIndex);
            String articleDatas = "{\"type\":\"weibo\",\"data\":" + JSON.toJSON(subWeibos).toString() + "}";
            long start = Calendar.getInstance().getTimeInMillis();
            //if (11 < downloadType && downloadType < 42) {//weibo
            if (5 == downloadType) {//weibo
                //发送到快速通道的数据
                Article2KafkaDetectTopic.sendNews_priority1(articleDatas);
            } else {
                Article2KafkaDetectTopic.sendNews(articleDatas);
            }
            long end = Calendar.getInstance().getTimeInMillis();
            logger.info(Thread.currentThread() + "###send to article_detect_data,type=weibo,size=" + subWeibos.size() + ",cost:" + (end - start) + "ms");
        }
    }


    public static void changeArticleMid(Article article) {
        String mid = article.getMid();
        String uid = article.getUid();
        String crawId = article.getCrawler_site_id();
        String downloadType = article.getDownload_type();
        String force = article.getForce();
        boolean flag = true;
        try {

            if (!"2".equals(force)) {
                //if (! "1001".equals(downloadType)) {
                if (article.getArticle_type() == 0 && (!"110".equals(downloadType))) {
                    article.setMid(MD5(mid) + getData(article.getCreated_at()) + mid);//用于mid识别微博，在汉明码阶段使用
                } else {
                    article.setMid(MD5(mid) + mid);
                }
            }

            if (StringUtils.isNotBlank(uid) && (11 != article.getArticle_type())) {
                article.setUid(MD5(uid) + uid);
            }
            /*//对app、twitter、facebook类uid不用加密
            if (StringUtils.isBlank(article.getUid()) || 11 == article.getArticle_type() || "999999".equals(article.getCrawler_site_id())
                    || "3115345".equals(article.getCrawler_site_id())) {

            } else {
                article.setUid(article.getCrawler_site_id() + "_" + article.getUid());
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static String MD5(String str) {
        if (str == null) {
            str = "";
        }

        try {
            return Md5Utils.getMd5ByStr(str).substring(0, 4);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return str;
        }
    }

    // public static void main(String[] args) {
    // ChangeMidUtils c = new ChangeMidUtils();
    // String md5 = c.MD5("0_3948867035028915");
    // logger.info(md5);
    //
    // }
    private static boolean isOrEncipherment(Article article) {
        boolean flag = true;
        if (StringUtils.isBlank(article.getUid())) flag = false;
        if (11 == article.getArticle_type()) flag = false;
        if ("999999".equals(article.getCrawler_site_id())) flag = false;
        if ("3115345".equals(article.getCrawler_site_id())) flag = false;
        return flag;
    }

    private static String getData(Timestamp created_at) {
        if (created_at == null) {
            return "";
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
        String timeStr = df.format(created_at);
        return timeStr;
    }

    @Test
    public void testtt() {
        Long s = 1542612725000L;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String timeStr = df.format(s);
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(s);//爬取时间
        Date newTime = Calendar.getInstance().getTime();//当前时间
        String ti = df2.format(c.getTime());

    /*    String mid = "10009_6610940546863071757";
        String md = MD5(mid) + mid;*/
        System.out.println(timeStr);

    }

    @Test
    public void testt() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("me", 0);
        jsonObject.put("mee", "1");
        String me = jsonObject.getString("me");
        String mee = jsonObject.getString("mee");
        JSONArray jsonArray=jsonObject.getJSONArray("you");
        System.out.println(me+":"+mee);
        if(null != jsonArray){
            System.out.println(jsonArray);

        }


    }

    @Test
    public void testttd() {
        String mid = "999999_3233665c00382ba9440911553e9035aa";
        String url = "https://twitter.com/SaidaElAlloumi/status/1097907885370195968";
        // String mid = "5425a101224fce8c";
        //(MD5(mid) + mid)
        //Long s = 1545724783000L;
        //String d = getData(new Timestamp(s));
        String midd = MD5(url) + mid;
        System.out.println(midd);

    }

    @Test
    public void testty() {
        int[] indices = new int[]{1, 3, 5, 6};
        int[] ss = new int[8];
        for (int indice : indices) {
            ss[indice] = 1;
        }//有值的位置设置为1
        for (int i = 0; i < ss.length; i++) {

            System.out.println("i:" + i + "zhi" + ss[i]);
        }


    }

    @Test
    public void listtest() {
        List<JSONObject> li = new ArrayList<>();
        JSONObject jd = new JSONObject();
        jd.put("mid", "112");
        jd.put("key", "334");
        JSONObject jd1 = new JSONObject();
        jd1.put("mid", "1122");
        jd1.put("key", "3324");

        li.add(jd);
        li.add(jd1);
        System.out.println(JSON.toJSONString(li));
    }

    @Test
    public void tett() {
        DateFormat df3 = new SimpleDateFormat("yyyy-MM-dd");
        Date dd = new Date();

        try {
            String t = "2018-09-16";
            String t1 = "2018-9-16";

            System.out.println(df3.parse(t).getTime());
            System.out.println(df3.parse(t1).getTime());

        } catch (ParseException e) {
            e.printStackTrace();
        }
  /*      try {
            if (df3.parse(t).getTime() < dd.getTime()) {
                System.out.println(true);
            }
            System.out.println(t.substring(t.indexOf("_") + 1));
        } catch (ParseException e) {
            e.printStackTrace();
        }*/
        //Date d= "Sat Oct 20 00:00:00 CST 2018";
    }

    @Test
    public void tdd() throws ParseException {
        NumberFormat nf = NumberFormat.getNumberInstance();
/*        Article article = new Article();
        article.setArticle_type(3);
        Integer i = article.getMe();
        System.out.println(i);
        if (0 == article.getMe()) {
            System.out.println(1);
        }*/
        String url = "http://www.aboluowang.com/2019/0222/1249995.html";
        String mid2 = Md5Utils.getMd5ByStr(url);
        System.out.println(mid2);
        String craw = "2019-04-08 16:05:45.198";
        //Date date=new Date(Date.parse(craw));

        System.out.println(df2.parse(craw).getTime());

        nf.setMaximumFractionDigits(5);
        System.out.println(nf.format(0L));
    }

    @Test
    public void tddd() {
        String orgTags = "[[\"001-000\",\"002-001\"],[\"001-002\",\"002-001\"],[\"001-001\",\"002-002-001\"]]";
        String s = ",";
        if (org.apache.commons.lang.StringUtils.isNotBlank(orgTags)) {
            // List<List> alarmAccounts = JSONArray.parseArray(orgTags, List.class);
            List<String> subjectList = Arrays.asList(s.split(","));
            System.out.println(subjectList);
            if (subjectList.contains("1")) {
                System.out.println("trye");
            }
/*            for(String ss:subjectList){
                System.out.println(";"+ss);
                if(StringUtils.isNotBlank(ss)){
                    System.out.println(ss);
                }
            }*/

            /*if (alarmAccounts.size() > 0) {
                for (List s : alarmAccounts) {
                    //if (s.size()>0) {
                        for(Object ss:s)
                            if(ss.equals("001-000")){
                                System.out.println(true);

                            }
                    //}
                }
            }*/
        }
    }

    @Test
    public void testjson() {
        Map<String, Object> map = new HashMap<>(16);
        map.put("code", 10001);
        map.put("message", "获取数据失败！");
        map.put("data", null);
        String j = JSONObject.toJSONString(map);

        int i = 8;
        if (i < 15) {
            System.out.println(1);
        } else if (i < 20) {
            System.out.println(j);

        }


    }

    @Test
    public void dd() {
        //ConcurrentHashMap<String,Object> countMap=new ConcurrentHashMap<>();
        HashMap<String, Object> countMap = new HashMap<>();
        countMap.put("2018-12-13_101", 74068L);

        countMap.put("2018-12-26_102", 74068L);
        countMap.put("2018-12-12_1013", 74068L);
        countMap.put("2018-12-14_k3", 74068L);
        countMap.put("2019-01-05", null);
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_YEAR, -3);
        Long befday = c.getTimeInMillis();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String bef = df.format(befday);
/*        System.out.println(bef);
        if("2019-01-05".contains(bef)){
            System.out.println(true);
        }
        for (String k : countMap.keySet()) {
            if (k.contains(bef)) {
                //countMap.remove(k);//移除过期的key
                countMap.put(k,null);
            }
        }
        //System.out.println("输出的结果是：" + countMap.keySet().contains(bef));
        for (String k : countMap.keySet()) {
            System.out.println(k);
        }*/
        Set<String> s = new HashSet<>();
        s.add("1");
        if (s != null) {
            s.remove("2");
            System.out.println(s);
        }

        //JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(countMap), JSONObject.class);
        //JSONObject jsonObject = JSONUtils.toJSONString(countMap);

    }
}
