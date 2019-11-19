package com.cloudera.vms.utils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;

import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.vms.bean.HotTopic;
import com.cloudera.vms.service.HotTopicService;

import scala.Tuple2;

public class TFIDF {

	private static final String folder = "/home/izhonghong/test/";

	public static double getSimilarity(double[] v1, double[] v2) {
		double mutiple = 0;
		for (int i = 0; i < v1.length; i++) {
			mutiple = mutiple + v1[i] * v2[i];
		}
		double abs1 = 0;
		for (int i = 0; i < v1.length; i++) {
			abs1 = abs1 + v1[i] * v1[i];
		}
		abs1 = Math.sqrt(abs1);

		double abs2 = 0;
		for (int i = 0; i < v2.length; i++) {
			abs2 = abs2 + v2[i] * v2[i];
		}
		abs2 = Math.sqrt(abs2);
		double abs = abs1 * abs2;
		double result = mutiple / abs;
		return result;
	}

	public static Long getHanming(double[] fature) {

		Long hanmingCode = 0l;

		for (double v : fature) {

			int sim = 0;
			if (v - 0 > 0.001) {
				sim = 1;
			}
			hanmingCode = (hanmingCode << 1) + sim;
		}
		return hanmingCode;
	}

	public static int getHanminDistance(byte[] v1, byte[] v2) {
		int distance = 0;

		for (int i = 0; i < v1.length; i++) {
			// distance+=v1[i]^v2[i];
			if (v1[i] != v2[i]) {
				distance++;
			}
		}
		return distance;
	}

	public static List<Long> saveTFIDF(List<String> weibos) throws IOException {

		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		long start = Calendar.getInstance().getTimeInMillis();
		List<Row> records = new ArrayList<Row>();
		for (String weibo : weibos) {
			records.add(RowFactory.create(0.0, WordSegmentationUtils.getTerms(weibo)));
		}
		JavaRDD<Row> jrdd = jsc.parallelize(records);

		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
		sentenceData.select("*").show();
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);
		wordsData.select("*").show(false);
		int numFeatures = 64;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(numFeatures);
		DataFrame featurizedData = hashingTF.transform(wordsData);

		featurizedData.select("*").show(false);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);
		rescaledData.select("*").show(false);
		double[][] vectors = new double[records.size()][];
		int i = 0;
		for (Row r : rescaledData.select("features", "label").take(records.size())) {
			Vector feature = r.getAs(0);

			double[] vals = feature.toArray();
			vectors[i++] = vals;
			// System.out.println();
			// Double label = r.getDouble(1);
			// System.out.println(feature);
			// System.out.println(feature.size());
			// System.out.println(label);
		}
		// double similarity01 = getSimilarity(vectors[0],vectors[1]);
		// double similarity12 = getSimilarity(vectors[1],vectors[2]);
		// double similarity02 = getSimilarity(vectors[0],vectors[2]);
		// System.out.println(similarity01);
		// System.out.println(similarity12);
		// System.out.println(similarity02);
		StringBuilder sb = new StringBuilder();
		File file = new File("d:\\peter\\vector-samples.txt");
		if (file.exists()) {
			file.delete();
		}
		List<Long> hanmins = new ArrayList<Long>();
		for (int m = 0; m < records.size(); m++) {
			Long hanmin = 0l;
			for (double v : vectors[m]) {
				int sim = 0;
				if (v - 0 > 0.001) {
					sim = 1;
				}
				hanmin = (hanmin << 1) + sim;
				sb.append(sim + " ");
			}
			hanmins.add(hanmin);
			FileUtils.write2File("d:\\peter\\vector-samples.txt", sb.toString(), "utf-8", true);
			sb.delete(0, sb.length());
		}
		System.out.println("weibo size:" + weibos.size());
		int m = 0;
		// for(int j=0;j<weibos.size();j++){
		// for(int k=0;k<weibos.size();k++){
		// if(k>j){
		// getSimilarity(vectors[j],vectors[k]);
		// }
		// m++;
		//// if(m%1000==0){
		//// System.out.println(m);
		//// }
		//
		// }
		// }
		long end = Calendar.getInstance().getTimeInMillis();

		System.out.println(end - start);
		jsc.stop();
		return hanmins;
	}

	public static void computeHanminDistance(List<Long> weibos) {

	}

	public static short getHanmingDistanceOld(long a, long b) {

		long c = a ^ b;
		short distance = 0;
		byte[] items = Long.toBinaryString(c).getBytes();
		for (byte item : items) {
			if (item == 49) {
				distance++;
			}
		}
		return distance;
	}

	public static short getHanmingDistance(long a, long b) {

		Long c = a ^ b;
		short distance = 0;
		for (int i = 0; i < 64; i++) {
			distance += c & 1;
			c = c >> 1;
		}
		return distance;

	}

	public static short notZoreBitCount(long a) {
		short count = 0;
		for (int i = 0; i < 64; i++) {
			count += a & 1;
			a = a >> 1;
		}
		return count;
	}

	public static String longToHex(long l) {
		return Long.toString(l, 16);
	}

	public static void testHanminDistance2(List<Long> weibos, List<String> records) throws IOException {

		long start = Calendar.getInstance().getTimeInMillis();
		short[][] distances = new short[weibos.size()][];
		for (int m = 0; m < weibos.size(); m++) {
			distances[m] = new short[weibos.size()];
			for (int n = 0; n < m; n++) {
				int distance = getHanmingDistance(weibos.get(m), weibos.get(n));
				distances[m][n] = (short) distance;
				// System.out.println(m+"-"+n+":"+distance);
				// if(distance<10){
				// System.out.println("distance:"+distance);
				// System.out.println("m:"+m+"|"+weibos.get(m));
				// System.out.println("n:"+n+"|"+weibos.get(n));
				// }

			}
		}
		for (int m = 0; m < weibos.size(); m++) {
			for (int n = weibos.size() - 1; n > m; n--) {

				distances[m][n] = distances[n][m];

			}
		}

		for (int m = 0; m < weibos.size(); m++) {
			distances[m][m] = (short) 0;
		}
		List<List<Short>> groups = new ArrayList<List<Short>>();
		Set<Short> grouped = new HashSet<Short>();
		for (int k = 0; k < weibos.size(); k++) {
			if (grouped.contains((short) k)) {
				continue;
			} else {
				grouped.add((short) k);
			}
			List<Short> group = new ArrayList<Short>();
			group.add((short) k);
			groups.add(group);
			for (int l = k + 1; l < weibos.size(); l++) {
				if (grouped.contains((short) l)) {
					continue;
				}
				int distance = distances[k][l];
				if (distance < 5) {
					group.add((short) l);
					grouped.add((short) l);
				}

			}
		}
		int u = 0;
		for (List<Short> group : groups) {
			if (group.size() > 1) {
				System.out.println("group(" + (++u) + ")【" + group.size() + "篇】");
				for (Short item : group) {
					System.out.println(item + ":" + records.get(item));
				}
			}

		}

		long end = Calendar.getInstance().getTimeInMillis();
		System.out.println("compute hanmin distance cost " + (end - start));

	}

	public static void testHanminDistance(List<String> weibos) throws IOException {

		List<String> records = FileUtils.readSource("d:\\peter\\vector-samples.txt", false);
		byte[][] vectors = new byte[records.size()][];
		int i = 0;
		for (String record : records) {

			String[] fields = record.split("\\s");
			vectors[i] = new byte[fields.length];
			for (int j = 0; j < fields.length; j++) {
				vectors[i][j] = Byte.parseByte(fields[j]);
			}
			i++;
		}
		long start = Calendar.getInstance().getTimeInMillis();
		short[][] distances = new short[weibos.size()][];
		for (int m = 0; m < records.size(); m++) {
			distances[m] = new short[weibos.size()];
			for (int n = 0; n < m; n++) {
				int distance = getHanminDistance(vectors[m], vectors[n]);
				distances[m][n] = (short) distance;
				// System.out.println(m+"-"+n+":"+distance);
				// if(distance<10){
				// System.out.println("distance:"+distance);
				// System.out.println("m:"+m+"|"+weibos.get(m));
				// System.out.println("n:"+n+"|"+weibos.get(n));
				// }

			}
		}
		for (int m = 0; m < records.size(); m++) {
			for (int n = records.size() - 1; n > m; n--) {

				distances[m][n] = distances[n][m];

			}
		}

		for (int m = 0; m < records.size(); m++) {
			distances[m][m] = (short) 0;
		}
		/**
		 * List<List<Short>> groups = new ArrayList<List<Short>>(); Set<Short> grouped =
		 * new HashSet<Short>(); for(int k=0;k<records.size();k++){
		 * if(grouped.contains((short) k)){ continue; }else{ grouped.add((short) k); }
		 * List<Short> group = new ArrayList<Short>(); group.add((short)k);
		 * groups.add(group); for(int l=k+1;l<records.size();l++){
		 * if(grouped.contains((short) l)){ continue; } int distance =
		 * getHanminDistance(vectors[k], vectors[l]); if(distance<5){
		 * group.add((short)l); grouped.add((short)l); }
		 * 
		 * } } int u=0; for(List<Short> group:groups){ if(group.size()>1){
		 * System.out.println("group("+(++u)+")【"+group.size()+"篇】"); for(Short
		 * item:group){ System.out.println(item+":"+weibos.get(item)); } }
		 * 
		 * 
		 * }
		 **/
		long end = Calendar.getInstance().getTimeInMillis();
		System.out.println("compute hanmin distance cost " + (end - start));

	}

	public static void main(String[] args) throws IOException, SQLException {
		System.out.println(notZoreBitCount(2));
		System.out.println(notZoreBitCount(10));
		System.out.println(notZoreBitCount(11));
		System.out.println(notZoreBitCount(Long.parseLong("01111111111111111111111111111111", 2)));
		System.out.println(Long.parseLong("dd6e33492ffa", 16));

		int distance = getHanmingDistance(Long.parseLong("01111111111111111111111111111111", 2),
				Long.parseLong("11111111111111111111111111111110", 2));
		System.out.println(distance);
		// System.out.println(Long.toBinaryString(a));
		int sum = 0;
		List<HotTopic> hotTopics = null;
		for (int i = 0; i < 1; i++) {
			List<String> sources = DBUtil.getWeiboList(1000);
			List<String> weibos = new ArrayList<String>();
			weibos.add("这网上的资料真少啊,晕!我刚好也在网上查汉明距的定义。我从书本上找到了了相关解释,看来还得多翻翻书才行");
			weibos.add("我刚好也在网上查汉明距的定义。我从书本上找到了了相关解释,看来还得多翻翻书才行,这网上的资料真少");

			for (String weibo : sources) {
				String s = weibo.replaceAll("(//@[\\S]+:)|(@[\\S]+\\s)|(\\[花心\\])", "");

				s = s.replaceAll("\\s+", "");
				if (s.length() > 30) {
					weibos.add(weibo);
				}

			}
			for (int k = 0; k < weibos.size(); k++) {
				System.out.println(k + ":" + weibos.get(k));
			}
			long start = Calendar.getInstance().getTimeInMillis();
			// KafkaTest.produce("weibo-simple2", weibos);
			KafkaTest.produce("my-replicated-topic", weibos);
			long end = Calendar.getInstance().getTimeInMillis();
			System.out.println("send " + weibos.size() + " weibos to kafka costs," + (end - start));
			sum += weibos.size();

			List<Long> hanmins = TFIDF.saveTFIDF(weibos);

			for (Long hanming : hanmins) {
				System.out.println(Long.toBinaryString(hanming));
			}

			// testHanminDistance(weibos);
			testHanminDistance2(hanmins, weibos);

			List<Tuple2<String, Long>> hanmings = new ArrayList<Tuple2<String, Long>>();
			for (int k = 0; k < hanmins.size(); k++) {
				hanmings.add(new Tuple2(k + "", hanmins.get(k)));
			}
			hotTopics = HotTopicService.getTopN(hanmings, 10);
			for (HotTopic hotTopic : hotTopics) {
				System.out.println(hotTopic.getMid() + "," + hotTopic.getHotValue());
			}
		}

		// List<HotTopic> hotTopics2 = null;
		// for(int i=0;i<1;i++){
		// List<String> sources = DBUtil.getWeiboList(5000);
		// List<String> weibos = new ArrayList<String>();
		// weibos.add("这网上的资料真少啊,晕!我刚好也在网上查汉明距的定义。我从书本上找到了了相关解释,看来还得多翻翻书才行");
		// weibos.add("我刚好也在网上查汉明距的定义。我从书本上找到了了相关解释,看来还得多翻翻书才行,这网上的资料真少");
		//
		// for(String weibo:sources){
		// String s = weibo.replaceAll("(//@[\\S]+:)|(@[\\S]+\\s)|(\\[花心\\])", "");
		//
		// s = s.replaceAll("\\s+", "");
		// if(s.length()>30){
		// weibos.add(weibo);
		// }
		//
		// }
		// long start = Calendar.getInstance().getTimeInMillis();
		// KafkaTest.produce("weibo-simple2", weibos);
		// long end = Calendar.getInstance().getTimeInMillis();
		// System.out.println("send "+weibos.size() +" weibos to kafka
		// costs,"+(end-start));
		// sum+=weibos.size();
		//
		// List<Long> hanmins = TFIDF.saveTFIDF(weibos);
		//
		// for(Long hanming:hanmins){
		// System.out.println(Long.toBinaryString(hanming));
		// }
		//
		// //testHanminDistance(weibos);
		// testHanminDistance2(hanmins,weibos);
		//
		// List<Tuple2<String,Long>> hanmings =new ArrayList<Tuple2<String,Long>>();
		// for(int k=0;k<hanmins.size();k++){
		// hanmings.add(new Tuple2(k+"",hanmins.get(k)));
		// }
		// hotTopics2 = HotTopicService.getTopN(hanmings, 10);
		//
		// }
		//
		// List<HotTopic> hotTopics3 = HotTopicService.getTopN(hotTopics, hotTopics2,
		// 0.1, 10);
		// for(HotTopic hotTopic:hotTopics3){
		// System.out.println(hotTopic.getMid()+","+hotTopic.getHotValue());
		// }
		// System.out.println("sum:"+sum);
		//

	}

}
