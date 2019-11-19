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

import static org.elasticsearch.spark.rdd.Metadata.ID;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.SparseVector;
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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.cloudera.vms.Config;
import com.cloudera.vms.similarity.GroupSimilarityComputer;
import com.cloudera.vms.similarity.SimilarityComputer;
import com.cloudera.vms.similarity.StayFocusComputer;
import com.cloudera.vms.similarity.alert.HotTopicCondLoader;
import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;
import com.cloudera.vms.similarity.listener.HotTopicSimilarityListener;
import com.cloudera.vms.similarity.policy.SimpleVotePolicy;
import com.cloudera.vms.similarity.stay_focus.StayFocusCond;
import com.cloudera.vms.similarity.stay_focus.StayFocusCondLoader;
import com.cloudera.vms.utils.HanmingCode;
import com.cloudera.vms.utils.KafkaTest;
import com.cloudera.vms.utils.WordSegmentationUtils;
import com.izhonghong.vms.zookeeper.ZKUtils;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 先计算hanmingcode 然后根据hanmingcode计算相似分组<br>
 * 目前只计算一个客户的数据<br>
 * 
 * 满足阈值 提供预警通知<br>
 * 
 * 阈值 通过ams设置 存放在缓存中<br>
 * 
 * @author lijl@izhonghong.com
 *
 */

public class SimilarityJob {

	private static final Logger logger = LoggerFactory.getLogger(SimilarityJob.class);
	private static final int numFeatures = 448;// 特征向量的维度
	private static final String ES_INDEX = "ams_data-";// 存入es的索引名称
	private static final String ES_WEIBO_TYPE = "/t_status_weibo";
	private static final String ES_ARTICLE_TYPE = "/t_article";

	public static boolean isWeiboId(String mid) {
		return mid.indexOf("_") == 17;
	}

	static JavaStreamingContext jsc;
	static SQLContext sqlContext;

	// static Broadcast<Map<String, SimilarityGroup>> groups;
	public static void main(String[] args) throws Exception {
		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "10");
		if (args.length < 6) {
			System.err.println(
					"Usage: ArticleHanmingCodeComputeJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
			System.exit(1);
		}

		String brokerList = args[0];
		final String group = args[1];
		String topic = args[2];
		int totalPartitions = Integer.parseInt(args[3]);
		int numThreads = Integer.parseInt(args[4]);
		int duration = Integer.parseInt(args[5]);

		// Create the context with 2 seconds batch size
		SparkConf sparkConf = getSparkConf();

		jsc = new JavaStreamingContext(sparkConf, new Duration(duration));
		sqlContext = new SQLContext(jsc.sparkContext());
		// jsc.checkpoint("./checkPoint");

		HashMap<String, String> kafkaParams = getKafkaParams(brokerList, group, topic, numThreads);
		Map<TopicAndPartition, Long> fromOffsets = getOffset(group, topic, totalPartitions);

		JavaInputDStream<JSONArray> jds = getKfkMsg(kafkaParams, fromOffsets, jsc);

		jds.foreachRDD(new VoidFunction<JavaRDD<JSONArray>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<JSONArray> rdd) throws Exception {
				long l = System.currentTimeMillis();
				// Map<String, SimilarityGroup> groups = SimilarityComputer.getGroups();//
				// 一次性获取分组信息
				// Broadcast<Map<String, SimilarityGroup>> broadcast =
				// jsc.sparkContext().broadcast(groups);
				// Map<String, SimilarityGroup> groups = null;//每条自己获取分组信息
				HotTopicCondLoader.refresh();
				StayFocusCondLoader.initCondHMCode(jsc, sqlContext);
				logger.info("[sim-code] get groups cost -> {}", System.currentTimeMillis() - l);
				similarityJob(group, rdd);
				// similarityJob(group, rdd, null);
			}
		});

		jsc.start();

		jsc.awaitTermination();
	}

	private static void saveOffset(final String group, OffsetRange[] offsets) throws Exception {
		Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();

		for (OffsetRange offsetRange : offsets) {
			long untilOffset = offsetRange.untilOffset();
			int partition = offsetRange.partition();
			partitionOffsets.put(partition, untilOffset);

		}
		ZKUtils.writeOffsetToZookeeper(group, offsets[0].topic(), partitionOffsets);
		partitionOffsets = ZKUtils.getPartitionOffset(group, offsets[0].topic());
		for (Integer partition : partitionOffsets.keySet()) {
			logger.info("[sim-job] save offet -> " + partition + ":" + partitionOffsets.get(partition));
		}
	}

	private static SparkConf getSparkConf() {
		SparkConf sparkConf = new SparkConf().setAppName("SimilarityJob");
		sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", Config.get(Config.KEY_ES_NODES));
		sparkConf.set("es.cluster.name", Config.get(Config.KEY_ES_CLUSTER_NAME));
		sparkConf.set("es.nodes.discovery", "true");
		sparkConf.set("es.index.refresh_interval", "30");
		sparkConf.set("es.write.operation", "upsert");
		sparkConf.set("es.port", Config.get(Config.KEY_ES_PORT));
		return sparkConf;
	}

	private static HashMap<String, String> getKafkaParams(String brokerList, final String group, String topic,
			int numThreads) {
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokerList);

		kafkaParams.put("group.id", group);
		kafkaParams.put("max.poll.records", "100");
		logger.info("[sim-job] kafkaParams:" + kafkaParams);
		return kafkaParams;
	}

	private static Map<TopicAndPartition, Long> getOffset(final String group, String topic, int totalPartitions)
			throws Exception {
		Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
		Map<Integer, Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
		Map<Integer, Long> earlistOffsets = null;
		try {
			earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
		} catch (Exception e) {
			logger.error("[sim-job] failed to get the partitions' offset, the topic:" + topic + " may doesn't exist");
			e.printStackTrace();
		}

		if (null != partitionOffsets && partitionOffsets.size() > 0) {
			for (Integer partition : partitionOffsets.keySet()) {
				long offset = partitionOffsets.get(partition);// todo partitionOffsets==null？
				long earlistOffset = earlistOffsets.get(partition);
				logger.info("[sim-job] get offset -> partition:" + partition + ",offset:" + offset + ",earlistOffset:"
						+ earlistOffset);

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
		return fromOffsets;
	}

	private static void similarityJob(final String group, JavaRDD<JSONArray> jsonRdd) throws Exception {
		long l = System.currentTimeMillis();
		logger.info("[sim-job]similarity job start...");
		OffsetRange[] offsets = ((HasOffsetRanges) jsonRdd.rdd()).offsetRanges();
		logger.info("[sim-job] offset range -> {}", Arrays.toString(offsets));
		// 过滤掉mid为空和正文长度小于10的文章
		JavaRDD<Row> jrdd = filterAndDistinctRow(jsonRdd);
		if (jrdd.count() == 0) {
			logger.info("[sim-job] no doc found !");
			return;
		}
		// mid , 空格分割的分词结果字符串, null, 正文, 创建时间
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("industry", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("message", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("created_at", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("dataJsonStr", DataTypes.StringType, false,
						org.apache.spark.sql.types.Metadata.empty()) }

		);

		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
		// sentenceData.select("*").show();
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);
		// wordsData.select("*").show(false);

		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(numFeatures);
		DataFrame featurizedData = hashingTF.transform(wordsData);

		// featurizedData.select("*").show(false);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);
		// rescaledData.select("*").show(false);

		JavaRDD<Row> rddRows = rescaledData.select("features", "id", "created_at", "dataJsonStr").toJavaRDD();
		logger.info("[sim-job] idf cost -> {}", System.currentTimeMillis() - l);
		l = System.currentTimeMillis();

		JavaRDD<Row> weiboRows = rddRows.filter(row -> isWeiboId(row.getString(1)));

		JavaRDD<Row> articleRows = rddRows.filter(row -> !isWeiboId(row.getString(1)));

		dealHMData(articleRows, ES_ARTICLE_TYPE);
		dealHMData(weiboRows, ES_WEIBO_TYPE);

		logger.info("[sim-job] index cost -> {}", System.currentTimeMillis() - l);

		saveOffset(group, offsets);

		logger.info("[sim-job] similarity job end.....");
	}

	/**
	 * 
	 * @param jrdd
	 * @param
	 * @return
	 */
	private static JavaRDD<Row> filterAndDistinctRow(JavaRDD<JSONArray> jrdd) {
		JavaRDD<Row> rowRdd = jrdd.filter(arr -> arr != null).flatMap(jsonArr -> {
			List<Row> rows = new ArrayList<>();

			jsonArr.parallelStream().forEach(obj -> {
				JSONObject json = JSON.parseObject(obj.toString());

				if (!json.containsKey("text"))
					return;
				try {

					String createDate = json.getString("created_at");
					if (StringUtils.isBlank(createDate)) {
						return;
					}
					String date;
					if (StringUtils.isNumeric(createDate)) {
						date = new DateTime(Long.parseLong(createDate)).toString("yyyy-MM-dd");
					} else {
						date = createDate.substring(0, 10);
					}
					
					String content = json.getString("text");
					content = content.replaceAll("(//@[\\S]+:)|(@[\\S]+\\s)|(\\[花心\\])", "");// format
					content = content.replaceAll("\\s+", "");
					if (content.length() > 1000) {
						content = content.substring(0, 1000);
					}
					Row row = RowFactory.create(json.getString("mid"), 
							WordSegmentationUtils.getTerms(content), 
							null,
							content, 
							date, 
							json.toJSONString());
					rows.add(row);
					// logger.info("[sim-job] add row mid -> {}", json.getString("mid"));
				} catch (IOException e) {
					logger.error("[sim-job] get terms failed !", e);
				}
			});
			return rows;
		});
		rowRdd = rowRdd.filter(row -> row != null && row.get(0) != null && row.getString(3).length() > 10)// 过滤非法数据
				.mapToPair(row -> new Tuple2<String, Row>(row.getString(0), row))// 转化为mid -> row 二元组
				.reduceByKey((row1, row2) -> row1)// 根据mid去重
				.map(tuple -> tuple._2());// 重新获取rdd<row>
		return rowRdd;
	}

	/**
	 * 过滤kafka消息 创建row<br>
	 * row -> mid , 空格分割的分词结果字符串, null, 正文, 创建时间, 文章jsonstring
	 * 
	 * @param kafkaParams
	 * @param fromOffsets
	 * @param jsc
	 * @return
	 */
	private static JavaInputDStream<JSONArray> getKfkMsg(HashMap<String, String> kafkaParams,
			Map<TopicAndPartition, Long> fromOffsets, JavaStreamingContext jsc) {
		JavaInputDStream<JSONArray> stream = KafkaUtils.createDirectStream(jsc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, JSONArray.class, kafkaParams, fromOffsets, data -> {
					String message = data.message();
					JSONObject json = JSON.parseObject(message);
					if (!json.containsKey("type")) {
						return null;
					}

					JSONArray jsonArray = json.getJSONArray("data");
					if (json.size() == 0)
						return null;
					return jsonArray;
				});
		return stream;
	}

	public static byte[] getHanmingCode(Row t) throws Exception {
		SparseVector feature = t.getAs(0);

		Field field = SparseVector.class.getDeclaredField("indices");
		field.setAccessible(true);
		int[] indices = (int[]) field.get(feature);

		double[] vals = new double[numFeatures];
		for (int indice : indices) {
			vals[indice] = 1;
		}

		byte[] hanmin = new byte[numFeatures / 8];
		byte b = 0;
		int k = 0;
		for (double v : vals) {
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

		return hanmin;
	}

	// 足浴组的匹配
	static GroupSimilarityComputer groupComputer = new GroupSimilarityComputer().votePolicy(new SimpleVotePolicy())
			// .filter(new HotTopicSimilarityFilter())
			.success(new HotTopicSimilarityListener());

	// 先自己分组 然后再和缓存的组匹配 "features", "id", "created_at"
	private static void dealHMData(JavaRDD<Row> rows, String esType) {
		JavaRDD<SimilarityMember> memberRows = row2SimMembers(rows);
		dealStayFocus(esType, memberRows);
		List<SimilarityGroup> selfGroups = selfGroup(memberRows);

		logger.info("[sim-job] new group size -> {}", selfGroups.size());

		Map<String, SimilarityGroup> groups = SimilarityComputer.getGroups();// 一次性获取分组信息

		Broadcast<Map<String, SimilarityGroup>> bcGroups = jsc.sparkContext().broadcast(groups);
		JavaRDD<SimilarityGroup> rdd = jsc.sparkContext().parallelize(selfGroups);

		logger.info("[sim-job] paitition size -> {}", rdd.getNumPartitions());
		JavaRDD<SimilarityGroup> groupedRdd = rdd.map(g -> {// 相似聚合
			SimilarityGroup group = groupComputer.compute(g, bcGroups.getValue());
			g.setCode(group.getCode());
			return g;
		});
		logger.info("[sim-job] group finish -> {}", rdd.getNumPartitions());
		groupedRdd = groupedRdd.mapPartitions(i -> {
			List<SimilarityGroup> list = new ArrayList<>();
			while (i.hasNext()) {
				SimilarityGroup g = i.next();
				Set<SimilarityMember> members = g.getMembers();
				members.forEach(m -> {
					m.setCode(g.getCode());
					m.setDocStr(null);
				});
				list.add(g);
			}
			return list;
		});
		logger.info("[sim job] format members finish...");
		// dealStayFocus(esType, groupedRdd);
		// logger.info("[sim job] stay focus finish...");
		dealSimGroup(esType, groupedRdd);
		logger.info("[sim job] sim group finish...");
		bcGroups.destroy();
	}

	private static void dealStayFocus(String esType, JavaRDD<SimilarityMember> members) {
		JavaRDD<SimilarityMember> needCorrect = members.map(StayFocusComputer::findNeedCorrectDoc)
				.filter(m -> m != null);
		JavaPairRDD<String, SimilarityMember> dateMembers = needCorrect.mapPartitionsToPair(i -> {
			List<Tuple2<String, SimilarityMember>> tuples = new ArrayList<>();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			while(i.hasNext()) {
				SimilarityMember m = i.next();
				tuples.add(new Tuple2<String, SimilarityMember>(df.format(m.getCreateDate()), m));
			}
			return tuples;
		}).repartition(5);
		dateMembers.reduceByKey((t1, t2) -> t1).map(t -> t._1).filter(date -> date != null).collect().forEach(date -> {
			String indexAndType = ES_INDEX + date + esType;
			JavaPairRDD<Object, Object> toEsPair = dateMembers.filter(t -> t._1.equals(date)).map(t -> t._2)
					.mapToPair(m -> {
						Map<Metadata, Object> meta = ImmutableMap.<Metadata, Object>of(ID, m.getMid());
						Map<String, ?> data = ImmutableMap.of("events_tag", m.getEventsTag());
						return new Tuple2<Object, Object>(meta, data);
					});
			JavaEsSpark.saveToEsWithMeta(toEsPair, indexAndType);
		});

	}


	private static void dealSimGroup(String esType, JavaRDD<SimilarityGroup> groupedRdd) {
		// 更新similarityCode 至 es
		JavaPairRDD<String, SimilarityMember> dateMembers = groupedRdd.flatMap(SimilarityGroup::getMembers)
				.mapToPair(m -> new Tuple2<String, SimilarityMember>(m.getCreatedDateStr(), m)).repartition(5);
		
		List<String> dates = dateMembers.reduceByKey((t1, t2) -> t1).map(t -> t._1).filter(date -> date != null).collect();
		logger.info("[ES index/type] {}", dates);
		dates.forEach(date -> {
			String indexAndType = ES_INDEX + date + esType;
			JavaPairRDD<Object, Object> toEsPair = dateMembers.filter(t -> date.equals(t._1)).mapToPair(t -> {
				SimilarityMember m = t._2();
				Map<Metadata, Object> meta = ImmutableMap.<Metadata, Object>of(ID, m.getMid());
				Map<String, ?> data = ImmutableMap.of("similarityCode", m.getCode());
				return new Tuple2<Object, Object>(meta, data);
			});
			JavaEsSpark.saveToEsWithMeta(toEsPair, indexAndType);
		});

	}

	private static List<SimilarityGroup> selfGroup(JavaRDD<SimilarityMember> memberRows) {

		List<SimilarityMember> list = memberRows.collect();
		List<SimilarityGroup> newGroups = new LinkedList<>();
		//list.forEach();

		list.stream().forEach(member -> {
			Optional<SimilarityGroup> first = newGroups.stream()
					.filter(group -> SimilarityComputer.isSimilarity(member.getHmCode(), group.getMeta())).findAny();
			if (first.isPresent()) {
				SimilarityGroup group = first.get();
				group.getMembers().add(member);

			} else {
				SimilarityGroup group = new SimilarityGroup();
				group.setMeta(member.getHmCode());
				Set<SimilarityMember> set = new HashSet<>();
				set.add(member);
				group.setMembers(set);
				newGroups.add(group);
			}

		});
		return newGroups;
	}

	private static JavaRDD<SimilarityMember> row2SimMembers(JavaRDD<Row> rows) {
//		return rows.mapPartitions(i -> {
//			List<SimilarityMember> members = new ArrayList<>();
//			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//			while(i.hasNext()) {
//				Row row = i.next();
//				byte[] hmBytes = getHanmingCode(row);
//				String hmCode = HanmingCode.encode(hmBytes);
//				SimilarityMember member = JSON.parseObject(row.getString(3), SimilarityMember.class);
//				member.setHmCode(hmCode);
//				member.setDocStr(row.getString(3));
//				member.setCreatedDateStr(df.format(member.getCreateDate()));
//				members.add(member);
//			}
//			return members;
//		});
		
		return rows.map(row -> {
			try {
				byte[] hmBytes = getHanmingCode(row);
				String hmCode = HanmingCode.encode(hmBytes);
				SimilarityMember member = JSON.parseObject(row.getString(3), SimilarityMember.class);
				member.setHmCode(hmCode);
				member.setDocStr(row.getString(3));
				member.setCreatedDateStr(row.getString(2));
				return member;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		});

	}

	// 预警相似计算
	static SimilarityComputer alertSimilarityComputer = new SimilarityComputer().votePolicy(new SimpleVotePolicy())
			// .filter(new HotTopicSimilarityFilter())
			.success(new HotTopicSimilarityListener());
}
