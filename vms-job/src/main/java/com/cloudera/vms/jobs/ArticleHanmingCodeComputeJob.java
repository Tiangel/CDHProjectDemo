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
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.cloudera.vms.Config;
import com.cloudera.vms.bean.ArticleHanmingCodeBean;
import com.cloudera.vms.utils.HanmingCode;
import com.cloudera.vms.utils.KafkaTest;
import com.cloudera.vms.utils.WordSegmentationUtils;
import com.izhonghong.vms.zookeeper.ZKUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.elasticsearch.spark.rdd.Metadata;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;
import scala.collection.Seq;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.spark.rdd.Metadata.ID;

/**
 * Consumes messages from topic in Kafka and does TF-IDF and hanming code computing.
 * <p>
 * Usage: HanmingCodeComputeJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>
 * <zkQuorums> is a list of one or more zookeeper servers that make quorum
 * <group> is the name of kafka consumer group
 * <topic> is a  kafka topic to consume from
 * <totalPartitions> total partitions of the topic
 * <numThreads> is the number of threads the kafka consumer should use
 * <duration>   The time interval at which streaming data will be divided into batches
 * To run this example:
 * sudo ./spark-submit  --class ArticleHanmingCodeComputeJob  --master spark://test11:7077  --executor-memory 4G --total-executor-cores 10 --jars (jars separated by ",") /home/izhonghong/test/spark-test/spark-test-0.0.1-SNAPSHOT.jar test11:2181,test12:2181,test13:2181 g3 weibo-simple2 4 4 5000
 */

public class ArticleHanmingCodeComputeJob {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger logger = LogManager.getLogger(ArticleHanmingCodeComputeJob.class);
    private static final int numFeatures = 448;//特征向量的维度
    private static final String ES_INDEX = "ams_data-";
    private static final String ES_WEIBO_TYPE = "/t_status_weibo";
    private static final String ES_ARTICLE_TYPE = "/t_article";
    private static DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static int maxSendSize = 50;

    public static boolean isWeiboId(String mid) {
        return mid.indexOf("_") == 17;
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: ArticleHanmingCodeComputeJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("ArticleHanmingCodeComputeJob");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("es.index.auto.create", "true");
        // sparkConf.set("es.nodes", "10.248.161.31");
        sparkConf.set("es.nodes", Config.get(Config.KEY_ES_NODES));
        sparkConf.set("es.cluster.name", Config.get(Config.KEY_ES_CLUSTER_NAME));

        sparkConf.set("es.nodes.discovery", "true");
        sparkConf.set("es.index.refresh_interval", "30");
        // sparkConf.set("es.batch.size.entries", "1000");
        sparkConf.set("es.write.operation", "upsert");
        sparkConf.set("es.port", Config.get(Config.KEY_ES_PORT));
        //sparkConf.set("es.mapping.id", "mid");
        // sparkConf.setMaster("local[4]");

        String brokerList = args[0];
        final String group = args[1];
        String topic = args[2];
        int totalPartitions = Integer.parseInt(args[3]);
        int numThreads = Integer.parseInt(args[4]);
        int duration = Integer.parseInt(args[5]);

        // Create the context with 2 seconds batch size

        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
//        jssc.checkpoint("/home/zhonghong/soft/spark-1.6.2-bin-hadoop2.6/jarbao/checkPoint");
        //jssc.sc().addJar("hdfs:///app/hduser1301/lib/hbase-client-1.2.2.jar");
        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        topicMap.put(topic, numThreads);


        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokerList);


        kafkaParams.put("group.id", group);
        kafkaParams.put("session.timeout.ms", "30000");

        kafkaParams.put("request.timeout.ms", "60000");
        kafkaParams.put("fetch.min.bytes", "1");
        //    JavaPairReceiverInputDStream<String, String> messages =
        //            KafkaUtils.createStream(jssc, args[0], group, topicMap);
        //
        //    JavaPairInputDStream<String, String> messages2 =
        //    KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
        Map<Integer, Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
        Map<Integer, Long> earlistOffsets = null;
        try {
            earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
        } catch (Exception e) {
            logger.error("failed to get the partitions' offset, the topic:" + topic + " may doesn't exist");
            e.printStackTrace();
        }

        if (null != partitionOffsets && partitionOffsets.size() > 0) {
            for (Integer partition : partitionOffsets.keySet()) {
                long offset = partitionOffsets.get(partition);//todo partitionOffsets==null？
                long earlistOffset = earlistOffsets.get(partition);
                logger.info("partition:" + partition + ",offset:" + offset + ",earlistOffset:" + earlistOffset);
                logger.info("partition:" + partition + ",offset:" + offset + ",earlistOffset:" + earlistOffset);

                if (earlistOffset > offset) {
                    offset = earlistOffset;
                }
                fromOffsets.put(new TopicAndPartition(topic, partition), offset);
            }
        } else {
            for (int i = 0; i < totalPartitions; i++) {
                fromOffsets.put(new TopicAndPartition(topic, i), earlistOffsets.get(i));
            }
        }
        logger.info("kafkaParams:" + kafkaParams);


        //create the direct stream, with witch we can maintain the offset in zookeeper by ourselves
        JavaInputDStream<Row> stream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, Row.class, kafkaParams, fromOffsets,
                new Function<MessageAndMetadata<String, String>, Row>() {
                    public Row call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        String message = v1.message();
                        String mid;
                        String content;
                        String created_at;
                        String crawler_time;
                        try {
                            if (message.startsWith("{")) {
                                ArticleHanmingCodeBean articleHanmingCodeBean = JSON.parseObject(message, ArticleHanmingCodeBean.class);
                                mid = articleHanmingCodeBean.getMid();
                                content = articleHanmingCodeBean.getText();
                                created_at = String.valueOf(articleHanmingCodeBean.getCreated_at());// 精确到日2019-03-10 15:53:04.0
                                crawler_time=articleHanmingCodeBean.getCrawler_time();
                                if(0 != Integer.valueOf(created_at.length()) && created_at.length()>10){
                                    created_at = created_at.substring(0, 10);
                                }else {
                                    return null;
                                }
                            } else {
                                String[] fields = message.split("\\|");
                                if (fields.length < 2) {
                                    return null;
                                }
                                mid = fields[0];
                                content = message.substring(message.indexOf("|") + 1);
                                created_at = "";
                                return null;
                            }
                            //                            logger.info(created_at+"|||||"+content);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                        try {
                            content = content.replaceAll("(//@[\\S]+:)|(@[\\S]+\\s)|(\\[花心\\])", "");//format
                            content = content.replaceAll("\\s+", "");
                            if (content.length() > 1000) {
                                content = content.substring(0, 1000);
                            }
                        } catch (NullPointerException e) {
                            content = "";
                            // TODO: handle exception
                        }
                        return RowFactory.create(mid, WordSegmentationUtils.getTerms(content), null, content, created_at,crawler_time);
                        //                        return RowFactory.create(mid, content, null, content);
                    }
                });


        logger.info("=========================lines count:" + stream.count());

        final SQLContext sqlContext = new SQLContext(jssc.sc());


        stream.foreachRDD(
                new Function<JavaRDD<Row>, Void>() {
                    public Void call(JavaRDD<Row> jrdd) throws Exception {
                        logger.info("#### start");
                        OffsetRange[] offsets = ((HasOffsetRanges) jrdd.rdd()).offsetRanges();

                        jrdd = jrdd.filter(new Function<Row, Boolean>() {

                            public Boolean call(Row v1) throws Exception {

                                //                        return v1 != null && v1.get(0) != null && v1.getString(3).length() > 10 && null != v1.getString(1) && v1.getString(1).length() > 1;
                                return v1 != null && v1.get(0) != null && v1.getString(3).length() > 10;
                            }
                        });
//                        		.repartition(10);

                        JavaPairRDD<String, Row> jp = jrdd.mapToPair(new PairFunction<Row, String, Row>() {

                            public Tuple2<String, Row> call(Row t) throws Exception {
                                return new Tuple2<String, Row>(t.getString(0), t);
                            }

                        });

                        jrdd = jp.reduceByKey(new Function2<Row, Row, Row>() {

                            public Row call(Row v1, Row v2) throws Exception {
                                // TODO Auto-generated method stub
                                return v1;
                            }

                        }).map(new Function<Tuple2<String, Row>, Row>() {

                            public Row call(Tuple2<String, Row> v1) throws Exception {
                                // TODO Auto-generated method stub
                                return v1._2();

                            }

                        });

                        StructType schema = new StructType(new StructField[]{
                                new StructField("id", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
                                new StructField("sentence", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
                                , new StructField("industry", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
                                , new StructField("message", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
                                , new StructField("created_at", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
                                , new StructField("crawler_time", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
                        });

                        logger.info("####count  begin");

                        int size = (int) jrdd.count();
                        logger.info("####weibo2 size:" + size);
                        if (size == 0) {
                            return null;
                        }
                        logger.info("jrdd partition size:" + jrdd.partitions().size());

                        DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
                        //sentenceData.select("*").show();
                        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
                        DataFrame wordsData = tokenizer.transform(sentenceData);
                        //wordsData.select("*").show(false);

                        HashingTF hashingTF = new HashingTF()
                                .setInputCol("words")
                                .setOutputCol("rawFeatures")
                                .setNumFeatures(numFeatures);//numFeatures为HashingTF类的成员变量默认为2^20，也就是hash的维数。
                        DataFrame featurizedData = hashingTF.transform(wordsData);

                        //featurizedData.select("*").show(false);
                        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
                        logger.info("idf:" + idf);
                        IDFModel idfModel = idf.fit(featurizedData);
                        DataFrame rescaledData = idfModel.transform(featurizedData);
                        //rescaledData.select("*").show(false);

                        JavaRDD<Row> rddRows = rescaledData.select("features", "id", "created_at", "crawler_time").toJavaRDD();

                        JavaRDD<Row> weiboRows = rddRows.filter(new Function<Row, Boolean>() {

                            public Boolean call(Row v1) throws Exception {
                                String mid = v1.getString(1);
                                return isWeiboId(mid);
                            }
                        });

                        JavaRDD<Row> articleRows = rddRows.filter(new Function<Row, Boolean>() {

                            public Boolean call(Row v1) throws Exception {
                                String mid = v1.getString(1);
                                return !isWeiboId(mid);
                            }

                        });

                        JavaPairRDD<String, Integer> articleCl = weiboRows(articleRows);//把数据的日期收集起来
                        for (Tuple2<String, Integer> tuple2 : articleCl.toArray()) {//遍历日期
                            saveEs(articleRows, tuple2._1, ES_ARTICLE_TYPE);

                        }

                        JavaPairRDD<String, Integer> weiboCl = weiboRows(weiboRows);//(2018-02-02,1)
                        for (Tuple2<String, Integer> tuple2 : weiboCl.toArray()) {//为了按天存储数据
                            saveEs(weiboRows, tuple2._1, ES_WEIBO_TYPE);
                        }
                        //发送到kafka
                        send2kafka(articleRows, ES_ARTICLE_TYPE);
                        send2kafka(weiboRows, ES_WEIBO_TYPE);

                        Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();

                        for (OffsetRange offsetRange : offsets) {
                            long untilOffset = offsetRange.untilOffset();
                            int partition = offsetRange.partition();
                            partitionOffsets.put(partition, untilOffset);

                        }
                        ZKUtils.writeOffsetToZookeeper(group, offsets[0].topic(), partitionOffsets);
                        partitionOffsets = ZKUtils.getPartitionOffset(group, offsets[0].topic());
                        for (Integer partition : partitionOffsets.keySet()) {
                            logger.info(partition + ":" + partitionOffsets.get(partition));
                        }

                        logger.info(Calendar.getInstance().getTimeInMillis() + ": "
                                + "compute hanmingDistance end...");
                        logger.info("#### end");
                        return null;
                    }
                });

        logger.info("finished..");

        // stream.print();

        jssc.start();

        jssc.awaitTermination();
    }


    public static String getHanmingCode(Row t) throws Exception {
        SparseVector feature = (SparseVector) t.getAs(0);//稀疏向量

        Field field = SparseVector.class.getDeclaredField("indices");

        field.setAccessible(true);
        int[] indices = (int[]) field.get(feature);//索引数组

        double[] vals = new double[numFeatures];
        for (int indice : indices) {
            vals[indice] = 1;
        }//有值的位置设置为1


        byte[] hanmin = new byte[numFeatures / 8];
        byte b = 0;
        int k = 0;
        for (double v : vals) {//各个索引
            int sim = 0;
            if (v - 0 > 0.001) {
                sim = 1;
            }
            k++;

            b = (byte) ((b << 1) + sim);
            if (k % 8 == 0) {
                hanmin[k / 8 - 1] = b;

                b = 0;
            }
        }

        String hanmingCode = HanmingCode.encode(hanmin);//将汉明码编码为汉字，以节省空间。
        return hanmingCode;
    }

    public static String getdate(int i) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(new Date().getTime() - (1000 * 60 * 60 * 24 * i));
        return df.format(date);
    }

    public static JavaPairRDD<String, Integer> weiboRows(JavaRDD<Row> articleRows) {
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.MONTH, -3);
        final Date date90 = instance.getTime();
        JavaPairRDD<String, Integer> weiboCl = articleRows.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                String createDate = row.getString(2);
                if (StringUtils.isBlank(createDate)) {
                    return false;
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                if (sdf.parse(createDate).after(date90)) {
                    return true;
                } else {
                    return false;
                }
            }
        }).mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row t) throws Exception {
                // TODO Auto-generated method stub
                String _1 = t.getString(2);
                return new Tuple2<String, Integer>(_1, 1);
            }


        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return 0;
            }
        });
        return weiboCl;
    }

    public static void send2kafka(JavaRDD<Row> articleRows, String type) {

        articleRows.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                try {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", Config.get(Config.KAFKA_SERVERS));
                    props.put("acks", "all");
                    props.put("retries", 1);
                    props.put("batch.size", 16384);
                    props.put("linger.ms", 1);
                    props.put("buffer.memory", 33554432);
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    Producer<String, String> producer = new KafkaProducer<String, String>(props);
                    List<JSONObject> list=new ArrayList<>();
                    while (rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        String hamingCode = getHanmingCode(row);
                        String id = row.getString(1);
                        JSONObject jsonObject = new JSONObject();
                        String create=row.getString(2);
                        String dateType = null;
                        jsonObject.put("mid", id);
                        jsonObject.put("hanmingCode", hamingCode);
                        //jsonObject.put("created_at", df.format(df.parse(create)));
                        jsonObject.put("created_at",create);
                        if (ES_ARTICLE_TYPE.equals(type)) {
                            dateType = "article";
                        } else if (ES_WEIBO_TYPE.equals(type)) {
                            dateType = "weibo";
                        }
                        jsonObject.put("type",dateType);
                        //object.put("data", jsonObject);
                        list.add(jsonObject);
                    }
                    int size=list.size();
                    if (size == 0) {
                        return;
                    }
                    int sendTimes = size % maxSendSize == 0 ? size / maxSendSize : size / maxSendSize + 1;
                    for(int i=0;i<sendTimes;i++){
                        int fromIndex = i * maxSendSize;
                        int toIndex = (i + 1)  * maxSendSize > size ? size : (i + 1) * maxSendSize;
                        List<JSONObject> subArticles = list.subList(fromIndex, toIndex);
                        String jsonArray = JSON.toJSON(subArticles).toString();
                        producer.send(new ProducerRecord<String, String>(Config.get(Config.KEY_KAFKA_HAMINGResult), jsonArray));
                    }
                    producer.close();
                }catch (Exception e){
                    logger.error("send hanmingCode to kafka error:"+e);
                }
            }
        });
    }

    public static void saveEs(JavaRDD<Row> articleRows, final String date, String type) {
        JavaRDD<Row> articleRows1 = articleRows.filter(new Function<Row, Boolean>() {

            public Boolean call(Row v1) throws Exception {
                String created_at = v1.getString(2);
                return created_at.equals(date);
            }

        });//为了按天存储数据
        JavaPairRDD<?, ?> esArticleData1 = articleRows1.mapToPair(new PairFunction<Row, Object, Object>() {
            public Tuple2<Object, Object> call(Row t) {
                try {
                    String id = t.getString(1);
                    String crawler_time=t.getString(3);
                    String hanmingCode = getHanmingCode(t);
                    Map<Metadata, Object> meta = ImmutableMap.<Metadata, Object>of(ID, id);
                    Map<String, String> data = ImmutableMap.of("hanmingCode", hanmingCode);
                    //Map<String, String> data = ImmutableMap.of("hanmingCode",hanmingCode,"crawler_time", crawler_time);
                    return new Tuple2<Object, Object>(meta, data);
                    //                     JavaEsSpark.saveJsonToEs(new Tuple2<Object, Object>(meta, data), Config.get(Config.KEY_ES_ARTICLE_URL));
                } catch (Exception e) {
                    return null;
                }
            }
        });
        try{
            String table = ES_INDEX + date + type;
            logger.info(table);
            JavaEsSpark.saveToEsWithMeta(esArticleData1, table);

        }catch (Exception e){
            logger.error("插入es错误："+e);
        }
    }


    /**
     * zcq
     *
     * @param articleRows
     * @return
     */
    public static JavaPairRDD<String, Iterable<Row>> getDateRows(JavaRDD<Row> articleRows) {
        JavaPairRDD<String, Iterable<Row>> records = articleRows.mapToPair(new PairFunction<Row, String, Row>() {

            public Tuple2<String, Row> call(Row t) throws Exception {
                // TODO Auto-generated method stub
                String date = t.getString(2);
                return new Tuple2<String, Row>(date, t);
            }

        }).groupByKey();

//        JavaPairRDD<Object, Object> javaPairRDD = records.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Object, Object>() {
//            public Tuple2<Object, Object> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
//                return null;
//            }
//        });

//        records.flatMapToPair()
        records.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Object, Object>() {
            public Tuple2<Object, Object> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
//                stringIterableTuple2.
                return null;
            }
        });

        for (Tuple2<String, Iterable<Row>> tuple2 : records.collect()) {
            String date = tuple2._1();
            Iterable<Row> rows = tuple2._2();
            Iterator<Row> iterator = rows.iterator();
            while (iterator.hasNext()) {
                try {
                    Row next = iterator.next();
                    String id = next.getString(1);
                    String hanmingCode = getHanmingCode(next);
                    Map<Metadata, Object> meta = ImmutableMap.<Metadata, Object>of(ID, id);
                    Map<String, ?> data = ImmutableMap.of("hanmingCode", hanmingCode);
                    Tuple2<Object, Object> tuple21 = new Tuple2<Object, Object>(meta, data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            SparkContext sc = new SparkContext();
            RDD<Object> parallelize = sc.parallelize((Seq<Object>) iterator, 2, null);
//            JavaEsSpark.saveToEs(parallelize,ES_INDEX + date + ES_ARTICLE_TYPE);
//			JavaEsSpark.saveToEsWithMeta(rows, ES_INDEX + date + ES_ARTICLE_TYPE);
        }
        return records;
    }

}
