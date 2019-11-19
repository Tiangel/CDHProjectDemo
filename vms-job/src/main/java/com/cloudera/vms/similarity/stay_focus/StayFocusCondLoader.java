
package com.cloudera.vms.similarity.stay_focus;
import com.alibaba.fastjson.JSON;
import com.cloudera.vms.Config;
import com.cloudera.vms.jobs.SimilarityJob;
import com.cloudera.vms.utils.HanmingCode;
import com.cloudera.vms.utils.JedisClusterUtils;
import com.cloudera.vms.utils.WordSegmentationUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class StayFocusCondLoader {
	private static final String redisKey = Config.get("stayfocus.cond.redis.key");
	private static Logger logger = LoggerFactory.getLogger(StayFocusCondLoader.class);
	static List<StayFocusCond> conds = null;
	static JedisCluster jedis = JedisClusterUtils.getJedisCluster();// 集群模式
	public static List<StayFocusCond> conds() {
		if (conds == null) {
			synchronized (StayFocusCondLoader.class) {
				if (conds == null) {
					refresh();
				}
			}
		}
		return conds;
	}

	public static void refresh() {
		if(conds == null)
			synchronized (StayFocusCondLoader.class) {
				List<StayFocusCond> tmp = new ArrayList<>();
				String cursor = "0";//游标，从0开始，遍历完返回0
				while (true) {

					ScanResult<Entry<String, String>> result = jedis.hscan(redisKey, cursor);
					cursor = result.getStringCursor();
					List<Entry<String, String>> entrys = result.getResult();

					if (entrys != null)
						entrys.forEach(e -> {
							StayFocusCond cond = JSON.parseObject(e.getValue(), StayFocusCond.class);
							tmp.add(cond);
						});

					if ("0".equals(cursor))
						break;
				}
				conds = tmp;
				logger.info("[sf loader] load new conds size -> {}", conds.size());
			}
	}

	public static void initCondHMCode(JavaStreamingContext jsc, SQLContext sqlContext) {
		List<StayFocusCond> sfc = conds();
		
		Map<String, StayFocusCond> map = sfc.stream().collect(Collectors.toMap(StayFocusCond::getId, c -> c));
		
		JavaRDD<StayFocusCond> sfcRdd = jsc.sparkContext().parallelize(sfc);
		JavaRDD<Row> rowRdd = sfcRdd
				.map(c -> RowFactory.create(c.getId(), WordSegmentationUtils.getTerms(c.getText())));

		// mid , 空格分割的分词结果字符串, null, 正文, 创建时间
		StructType schema = new StructType(new StructField[] {
				new StructField("eventId", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()) }
		);

		DataFrame sentenceData = sqlContext.createDataFrame(rowRdd, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);

		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(448);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);

		List<Row> rows = rescaledData.select("features", "eventId").toJavaRDD().collect();
		rows.forEach(row -> {
			
			try {
				byte[] bytes = SimilarityJob.getHanmingCode(row);
				
				String hmCode = HanmingCode.encode(bytes);
				
				map.get(row.get(1)).setHmCode(hmCode);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}
}
