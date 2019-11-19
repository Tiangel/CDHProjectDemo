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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
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
import org.apache.spark.mllib.linalg.Vector;
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
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableMap;
import com.cloudera.vms.Config;
import com.cloudera.vms.es.EsWeibo;
import com.cloudera.vms.hbase.HBaseJavaAPI;
import com.cloudera.vms.hbase.RowBean;
import com.cloudera.vms.utils.KafkaTest;
import com.cloudera.vms.utils.TFIDF;
import com.cloudera.vms.utils.WordSegmentationUtils;
import com.izhonghong.vms.zookeeper.ZKUtils;

import static org.elasticsearch.spark.rdd.Metadata.*;

import org.elasticsearch.spark.rdd.Metadata;    

/**
 * Consumes messages from topic in Kafka and does TF-IDF and hanming code computing.
 *
 * Usage: HanmingCodeComputeJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>
 *   <zkQuorums> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topic> is a  kafka topic to consume from
 *   <totalPartitions> total partitions of the topic
 *   <numThreads> is the number of threads the kafka consumer should use
 *   <duration>   The time interval at which streaming data will be divided into batches
 * To run this example:
 *   sudo ./spark-submit  --class HanmingCodeComputeJob  --master spark://test11:7077  --executor-memory 4G --total-executor-cores 10 --jars (jars separated by ",") /home/izhonghong/test/spark-test/spark-test-0.0.1-SNAPSHOT.jar test11:2181,test12:2181,test13:2181 g3 weibo-simple2 4 4 5000
 */

public  class SubjectHanmingCodeComputeJob {
	
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final Logger logger =  LogManager.getLogger(SubjectHanmingCodeComputeJob.class);
  
  private SubjectHanmingCodeComputeJob() {
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {
	  if (args.length < 6) {
	      System.err.println("Usage: HotTopicComputeJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
	      System.exit(1);
	  }


    SparkConf sparkConf = new SparkConf().setAppName("SubjectHanmingCodeComputeJob");
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
    //jssc.sc().addJar("hdfs:///app/hduser1301/lib/hbase-client-1.2.2.jar");
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    
    topicMap.put(topic, numThreads);


    
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list",brokerList);
   

    kafkaParams.put("group.id", group);
//    JavaPairReceiverInputDStream<String, String> messages =
//            KafkaUtils.createStream(jssc, args[0], group, topicMap);
//    
//    JavaPairInputDStream<String, String> messages2 =
//    KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    Map<kafka.common.TopicAndPartition, Long> fromOffsets = new HashMap<kafka.common.TopicAndPartition, Long>();
    Map<Integer,Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
    Map<Integer,Long> earlistOffsets = null; 
    try{
    	earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
    }catch(Exception e){
    	logger.error("failed to get the partitions' offset, the topic:"+topic+" may doesn't exist");
    }
    
    if(null!=partitionOffsets&&partitionOffsets.size()>0){
    	 for(Integer partition:partitionOffsets.keySet()){
    		    long offset = partitionOffsets.get(partition);
    		    long earlistOffset = earlistOffsets.get(partition);
    		    logger.info("partition:"+partition+",offset:"+offset+",earlistOffset:"+earlistOffset);   
    		    System.out.println("partition:"+partition+",offset:"+offset+",earlistOffset:"+earlistOffset);
    		    if(earlistOffset>offset){
    		    	offset=earlistOffset;
    		    }
    	    	fromOffsets.put(new TopicAndPartition(topic,partition), offset);
    	    }
    }else{
    	for(int i=0;i<totalPartitions;i++){
    		fromOffsets.put(new TopicAndPartition(topic,i), earlistOffsets.get(i));
    	}
    }
    System.out.println("kafkaParams:"+kafkaParams);
    
    

   //create the direct stream, with witch we can maintain the offset in zookeeper by ourselves
    JavaInputDStream<Row> stream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, Row.class, kafkaParams, fromOffsets, 
    		 new Function<MessageAndMetadata<String, String>, Row>() {  
                 public Row call(MessageAndMetadata<String, String> v1)  
                         throws Exception {  
                	 String message = v1.message();
                	 String[] fields = message.split("\\,");
                	 if(fields.length<2){
                		 return null;
                	 }
                	 String subjectId = fields[0];
                
                	 String content = null;
//                	if(fields.length==3){
//                		 content = message.substring(message.indexOf("|"+fields[1])+fields[1].length()+2);
//                	}else{
                		content = message.substring(message.indexOf(",")+1);
                	//}
                	
                	 if(content.length()>200){
                		 content = content.substring(0,200);
                	 }
                     return RowFactory.create(subjectId, WordSegmentationUtils.getTerms(content),null,content);  
                 }  
             });
    
  
//    JavaDStream<Row> lines =   messages2.map(new Function<Tuple2<String, String>, Row>() {
//        public Row call(Tuple2<String, String> tuple2) throws IOException {
//      	  long t = Calendar.getInstance().getTimeInMillis();
//          return RowFactory.create(t+"", WordSegmentationUtils.getTerms(tuple2._2().toString()));
//        }
//      });
//     
    
//    JavaDStream<Row> lines = messages.map(new Function<Tuple2<String, String>, Row>() {
//      public Row call(Tuple2<String, String> tuple2) throws IOException {
//    	  String message = tuple2._2;
//     	 String[] fields = message.split(",");
//     	 if(fields.length<3||fields[1].length()<1){
//     		 return null;
//     	 }
//     	 String mid = fields[0];
//     	 String region = fields[1];
//     	
//     	 String content = message.substring(message.indexOf(fields[1])+fields[1].length()+1);
//     	
//          return RowFactory.create(mid, WordSegmentationUtils.getTerms(content),region,content);  
//      }
//    });
//   
    

    System.out.println("=========================lines count:"+stream.count());
   
   	final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jssc.sc());



   	
   	stream.foreachRDD(new Function<JavaRDD<Row>, Void> (){
 
		public Void call(JavaRDD<Row> jrdd) throws Exception {
			  OffsetRange[] offsets = ((HasOffsetRanges) jrdd.rdd()).offsetRanges();   

			  jrdd = jrdd.filter(new Function<Row,Boolean>(){

					public Boolean call(Row v1) throws Exception {
						return v1!=null&&v1.get(0)!=null&&v1.getString(3).length()>10&&null!=v1.getString(1)&&v1.getString(1).length()>1;
					}
			   	});
			  
			  //remove the duplicate records
			  JavaPairRDD<String, Row> jp= jrdd.mapToPair(new PairFunction<Row, String, Row>() {

				public Tuple2<String, Row> call(Row t) throws Exception {
					return new Tuple2<String, Row>(t.getString(0), t);
				}
				  
			});
			  jrdd=jp.reduceByKey(new Function2<Row, Row, Row>(){

				public Row call(Row v1, Row v2) throws Exception {
					// TODO Auto-generated method stub
					return v1;
				}
				  
			  }).map(new Function<Tuple2<String, Row>, Row>(){

				public Row call(Tuple2<String, Row> v1) throws Exception {
					// TODO Auto-generated method stub
					return v1._2();
				}
				  
			  });
			 
			 

				 StructType schema = new StructType(new StructField[]{
				  		  new StructField("id", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
				  		  new StructField("sentence", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
				  		  ,new StructField("industry", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
				  		  ,new StructField("message", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
				  		});
			 
			System.out.println("####count  begin");
		
			int size= (int)jrdd.count();
			System.out.println("####weibo2 size:"+size);
			if(size==0){
				return null;
			}
			System.out.println("jrdd partition size:"+jrdd.partitions().size());
			  		
			DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
			//sentenceData.select("*").show();
			Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
			DataFrame wordsData = tokenizer.transform(sentenceData);
			//wordsData.select("*").show(false);
			int numFeatures =64;
			HashingTF hashingTF = new HashingTF()
			  .setInputCol("words")
			  .setOutputCol("rawFeatures")
			  .setNumFeatures(numFeatures);
			DataFrame featurizedData = hashingTF.transform(wordsData);
			
			//featurizedData.select("*").show(false);
			IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
			IDFModel idfModel = idf.fit(featurizedData);
			DataFrame rescaledData = idfModel.transform(featurizedData);
			//rescaledData.select("*").show(false);
		
		
			JavaRDD<Row> rddRows =rescaledData.select("features","id").toJavaRDD();
			
			JavaRDD<EsWeibo> rddHanmings = rddRows.map(new Function<Row, EsWeibo>() {
             public EsWeibo call(Row r) { 
            	 
            	  Vector feature = r.getAs(0);
            	  String id = r.getString(1);
    			  double[] vals = feature.toArray();
    			  Long hanmin= TFIDF.getHanming(vals);
    			  return new EsWeibo(id,hanmin);

            	  }
           });
		
			JavaPairRDD<?,?>  esData = rddRows.mapToPair(new PairFunction<Row,Object,Object>(){

				public Tuple2<Object, Object> call(Row t) throws Exception {
					 Vector feature = t.getAs(0);
	            	  String id = t.getString(1);
	    			  double[] vals = feature.toArray();
	    			  Long hanmingCode = TFIDF.getHanming(vals);
	    				String routing= id.substring(id.indexOf("|")+1);
	    			Map<Metadata, Object> meta = ImmutableMap.<Metadata, Object> of(ID,id,ROUTING,routing);
	    		
	    			String hanming = Long.toHexString(hanmingCode);
					Map<String, ?> data = ImmutableMap.of("score", hanmingCode,"hanmingCode",hanming);
					return  new Tuple2<Object, Object>(meta, data);
				}
				
			});

			
	
		
			List<EsWeibo> hanmings = rddHanmings.collect();
			System.out.println("save to hbase begin");
			rddHanmings.foreachPartition(new VoidFunction<Iterator<EsWeibo>>(){

				public void call(Iterator<EsWeibo> hanmingIterator)
						throws Exception {
					List<RowBean> rowBeans = new ArrayList<RowBean>();
					  //get hbase connection
					while(hanmingIterator.hasNext()){
						//persist hanmingCode
						RowBean rowBean = new RowBean();
						//Map<String,String> columns = new HashMap<String,String>();
						EsWeibo hanming = hanmingIterator.next();
						rowBean.setColumnFamily("fn");
						rowBean.setRow(hanming.getId());
						//System.out.println("weiboId:"+hanming.getId());
						rowBean.setColumn("hanmingCode");
						rowBean.setValue(hanming.getHanmingCode()+"");
					    rowBeans.add(rowBean);
						
					}
					HBaseJavaAPI.addRows(Config.get(Config.KEY_HBASE_SUBJECT_NAME), rowBeans);
					
				}
				
			});
		
			System.out.println("save to hbase end");
		
			System.out.println("save to es begin");
			//JavaEsSpark.saveToEs(rddHanmings, "spark4/docs");
			
			JavaEsSpark.saveToEsWithMeta(esData, Config.get(Config.KEY_ES_SUBJECT_URL));
			
			//JavaEsSpark.saveToEsWithMeta(esData, "peter/docs");
		
			
			System.out.println("save to es end");
			//按照汉明距离分类，汉明距离小于5的文章，化为同一类
//			for(int j=0;j<(10>size?size:10);j++){
//				Tuple2<String,Long> hanming = hanmings.get(j);
//				System.out.println("weiboId:"+hanming._1()+",hanmingCode:"+hanming._2());
//			}
//			System.out.println(Calendar.getInstance().getTimeInMillis()+": compute hanmingDistance begin...");
//			List<List<Short>> groups = new ArrayList<List<Short>>();
//			Set<Short> grouped = new HashSet<Short>();
//	        for(int k=0;k<(100>size?100:size);k++){
//	        	if(grouped.contains((short) k)){
//	    			continue;
//	    		}else{
//	    			grouped.add((short) k);
//	    		}
//	        	List<Short> group = new ArrayList<Short>();
//	        	group.add((short)k);
//	        	groups.add(group);
//	        
//	        	for(int l=k+1;l<(100>size?size:100);l++){
//	        		if(grouped.contains((short) l)){
//	        			continue;
//	        		}
//	        		int distance = TFIDF.getHanmingDistance(hanmings.get(k)._2, hanmings.get(l)._2);
//	        		if(distance<5){
//	        			group.add((short)l);
//	        			grouped.add((short)l);
//	        		}
//	        		
//	        	}
//	        
//	        }

	        
	        Map<Integer,Long> partitionOffsets = new HashMap<Integer,Long>();
	        
	        for(OffsetRange offsetRange:offsets){
	        	long untilOffset = offsetRange.untilOffset();
	        	int partition = offsetRange.partition();
	        	partitionOffsets.put(partition, untilOffset);
	  	       
	        }
	        ZKUtils.writeOffsetToZookeeper(group, offsets[0].topic(), partitionOffsets);
	        partitionOffsets = ZKUtils.getPartitionOffset(group, offsets[0].topic());
	        for(Integer partition:partitionOffsets.keySet()){
	        	System.out.println(partition+":"+partitionOffsets.get(partition));
	        }
	        
	    	System.out.println(Calendar.getInstance().getTimeInMillis()+": "
	    			+ "compute hanmingDistance end...");
			return null;
		}
    	
    });
  

    System.out.println("finished..");	
  		
    stream.print();

    jssc.start();
   
    jssc.awaitTermination();
  }
  
  

  
 
}
