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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
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
import org.apache.spark.api.java.function.PairFunction;
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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.cloudera.vms.Config;
import com.cloudera.vms.bean.HotTopic;
import com.cloudera.vms.service.HotTopicService;
import com.cloudera.vms.utils.CheckpointUtil;
import com.cloudera.vms.utils.KafkaTest;
import com.cloudera.vms.utils.TFIDF;
import com.cloudera.vms.utils.WordSegmentationUtils;
import com.izhonghong.vms.zookeeper.ZKUtils;

/**
 * Consumes messages from topic in Kafka and does hot topic computing.
 *
 * Usage: HotTopicComputeJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>
 *   <zkQuorums> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topic> is a  kafka topic to consume from
 *   <totalPartitions> total partitions of the topic
 *   <numThreads> is the number of threads the kafka consumer should use
 *   <duration>   The time interval at which streaming data will be divided into batches
 * To run this example:
 *   sudo ./spark-submit  --class HotTopicComputeJob  --master spark://test11:7077  --executor-memory 4G --total-executor-cores 10 --jars (jars separated by ",") /home/izhonghong/test/spark-test/spark-test-0.0.1-SNAPSHOT.jar test11:2181,test12:2181,test13:2181 g3 weibo-simple2 4 4 5000
 */

public  class HotTopicComputeJob {
	
  private static final Logger logger =  LogManager.getLogger(HotTopicComputeJob.class);
  private static final Pattern SPACE = Pattern.compile(" ");

  private static final int TFIDF_FEATURE_NUMBERS= 64;
  
  private HotTopicComputeJob() {
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {
	  
    if (args.length < 6) {
      System.err.println("Usage: HotTopicComputeJob <zkQuorums> <group> <topic> <totalPartitions> <numThreads> <duration>");
      System.exit(1);
    }
    final int topN = Integer.parseInt(Config.get(Config.KEY_CLUSTING_TOP_NUM));
    final double decay = Double.parseDouble(Config.get(Config.KEY_CLUSTINGE_DECAY));
    
    
    SparkConf sparkConf = new SparkConf().setAppName("HotTopicComputeJob");
    String zkQuorums = args[0];
    final String group = args[1];
    String topic = args[2];
    int totalPartitions = Integer.parseInt(args[3]);
    int numThreads = Integer.parseInt(args[4]);
    int duration = Integer.parseInt(args[5]);
    boolean checkPointsOn = true;
    if(args.length==7){    	
    	checkPointsOn = Boolean.parseBoolean(args[6]);
    }
    
    
    // Create the context with 2 seconds batch size
   
    final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
   
    
    
    topicMap.put(topic, numThreads);

    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")  ;
    
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list",zkQuorums);
   

    kafkaParams.put("group.id", group);
//    JavaPairReceiverInputDStream<String, String> messages =
//            KafkaUtils.createStream(jssc, args[0], group, topicMap);
    
//    JavaPairInputDStream<String, String> messages2 =
//    KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    //obtain the latest offsets from zookeeper
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
              
                	 //String mid = UUID.randomUUID().toString();   
                	 String message = v1.message();
                	 String[] fields = message.split("\\|");
                	 if(fields.length<3||fields[1].length()<1){
                		 return null;
                	 }
                	 String mid = fields[0];
                	 String region = fields[1];
                	
                	 String content = message.substring(message.indexOf("|"+fields[1])+fields[1].length()+2);
                	 content =  content.replaceAll("(//@[\\S]+:)|(@[\\S]+\\s)|(\\[花心\\])", "").replaceAll("\\s+", "");
                	 if(content.length()>140){
                		 content = content.substring(0,140);
                	 }
                	 if(content.length()<40){
                		 return null;
                	 }
                	 if(content.contains("天气")&&content.contains("℃")&&content.contains("风")){
                		 return null;
                	 }
                	
                     return RowFactory.create(mid, WordSegmentationUtils.getTerms(content),region,content);  
            }  
      });
    
    System.out.println("kafkaParams:"+kafkaParams);
    

    System.out.println("=========================lines count:"+stream.count());
   
   	final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jssc.sc());

   
   	final Map<String,List<HotTopic>> oldTopicsMap = new HashMap<String,List<HotTopic>>();
   	if(checkPointsOn){
   		Object  o = CheckpointUtil.get("topics");
   	    if(null!=o){
   	    	oldTopicsMap.clear();
   	    	oldTopicsMap.putAll((Map<String,List<HotTopic>>) o);
   	    	System.out.println("load topics from "+Config.get(Config.KEY_JOB_CHECKPOINT_URL)+"/topics, mapSize="+oldTopicsMap.size());
   	    	
   	    }
   	}
    
   	
   	stream.foreachRDD(new Function<JavaRDD<Row>, Void> (){
   	
 
		public Void call(JavaRDD<Row> jrdd) throws Exception {
		
			System.out.println("####count  begin");		
		int size= (int)jrdd.count();
		System.out.println("####weibo size:"+size);
		if(size==0){
			return null;
		}
			  OffsetRange[] offsets = ((HasOffsetRanges) jrdd.rdd()).offsetRanges();  
			  System.out.println("##offsets:");
			  for(OffsetRange offset:offsets){
				  System.out.println("######:"+offset.partition()+":"+offset.untilOffset());
			  }
			  
				jrdd = jrdd.filter(new Function<Row,Boolean>(){

					public Boolean call(Row v1) throws Exception {
						return v1!=null&&v1.getString(3).length()>40;
					}
			   	});
				System.out.println("###after filter");
				size= (int)jrdd.count();
				System.out.println("####filter weibo size:"+size);
				if(size==0){
					return null;
				}
		
			  JavaPairRDD<String, String> midIndustryIds = jrdd.mapToPair(new PairFunction<Row,String,String>(){

				public Tuple2<String, String> call(Row t) throws Exception {
				
					return new Tuple2<String,String>(t.getString(0),t.getString(2));
				}
				  
			  });
		
			  
			  
//			  JavaPairRDD<String, String> midMsgs = jrdd.mapToPair(new PairFunction<Row,String,String>(){
//
//					public Tuple2<String, String> call(Row t) throws Exception {
//					
//						return new Tuple2<String,String>(t.getString(0),t.getString(3));
//					}
//					  
//				  });
//			  List<Tuple2<String, String>> midMsgList = midMsgs.collect();
//			  Map<String,String> midMsgMap = new HashMap<String,String>();
//			  for(Tuple2<String,String> midMsg:midMsgList){
//				  
//				//  System.out.println(midIndustryIdTuple._1+":"+midIndustryIdTuple._2);
//				  midMsgMap.put(midMsg._1, midMsg._2);				  
//			  }
			  

			 StructType schema = new StructType(new StructField[]{
			  		  new StructField("id", DataTypes.StringType, false, Metadata.empty()),
			  		  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
			  		  ,new StructField("industry", DataTypes.StringType, false, Metadata.empty())
			  		,new StructField("message", DataTypes.StringType, false, Metadata.empty())
			  		});
			 

			System.out.println("jrdd partition size:"+jrdd.partitions().size());
			  		
			DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
			//sentenceData.select("*").show();
			Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
			DataFrame wordsData = tokenizer.transform(sentenceData);
			//wordsData.select("*").show(false);
			
			HashingTF hashingTF = new HashingTF()
			  .setInputCol("words")
			  .setOutputCol("rawFeatures")
			  .setNumFeatures(TFIDF_FEATURE_NUMBERS);
			DataFrame featurizedData = hashingTF.transform(wordsData);
			
			//featurizedData.select("*").show(false);
			IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
			IDFModel idfModel = idf.fit(featurizedData);
			DataFrame rescaledData = idfModel.transform(featurizedData);
			//rescaledData.select("*").show(false);
		
		
			JavaRDD<Row> rddRows =rescaledData.select("features","id").toJavaRDD().filter(new Function<Row,Boolean>(){

				public Boolean call(Row v1) throws Exception {
					return v1!=null;
				}
		   	});
			
			JavaRDD<Tuple2<String, Long>> rddHanmings = rddRows.map(new Function<Row, Tuple2<String, Long>>() {
             public Tuple2<String, Long> call(Row r) { 
            	 
            	 SparseVector feature = (SparseVector)r.getAs(0);
					try{
						 Field field=SparseVector.class.getDeclaredField("indices"); 
	            		  field.setAccessible(true);
	            		int[] indices = (int[]) field.get(feature);
	              	  String id = r.getString(1);
	              	
	      			  double[] vals = new double[64];
	      			  for(int indice:indices){
	      				 vals[indice]=1;
	      			  }
    			  Long hanmin= TFIDF.getHanming(vals);
    			  return new Tuple2<String,Long>(id,hanmin);
					}catch(Exception  e){
						e.printStackTrace();
						return null;
					}
					
            	  }
           });
		

			 long midIndustryIdsCount = midIndustryIds.count();
			  System.out.println("midIndustryIdsCount:"+midIndustryIdsCount);
			  System.out.println("###before midIndustryIds.collect() ");	  
		
			  List<Tuple2<String, String>> midIndustryIdList = midIndustryIds.collect();
			  System.out.println("###after midIndustryIds.collect() ");	  

			  Map<String,String> midIndustryIdMap = new HashMap<String,String>();
			  for(Tuple2<String,String> midIndustryIdTuple:midIndustryIdList){
				  
				//  System.out.println(midIndustryIdTuple._1+":"+midIndustryIdTuple._2);
				  midIndustryIdMap.put(midIndustryIdTuple._1, midIndustryIdTuple._2);				  
			  }
	
		
			List<Tuple2<String,Long>> hanmings = rddHanmings.collect();
			
			System.out.println(Calendar.getInstance().getTimeInMillis()+": compute hanmingDistance begin...");
			Map<String,List<Tuple2<String,Long>>> industryHanmings = new HashMap<String,List<Tuple2<String,Long>>>();
	
			for(Tuple2<String,Long> midHanmingTupe:hanmings){
				String mid = midHanmingTupe._1;
				String industryIds = midIndustryIdMap.get(mid);
		
				String[] fields = industryIds.split(",");
				for(String industryId:fields){
					List<Tuple2<String,Long>> oneIndustryHanmings = industryHanmings.get(industryId);
					if(null==oneIndustryHanmings){
						oneIndustryHanmings = new ArrayList<Tuple2<String,Long>>();
						industryHanmings.put(industryId, oneIndustryHanmings);
					}
					oneIndustryHanmings.add(midHanmingTupe);
				}
				
			}
			
			
			for(String industryId:industryHanmings.keySet()){
//				if(!industryId.endsWith("北京")){
//					continue;
//				}
				List<Tuple2<String,Long>> oneIndustryHanmings = industryHanmings.get(industryId);
				List<HotTopic> hotTopics = HotTopicService.getTopN(oneIndustryHanmings, topN);
				List<HotTopic> oldTopics = oldTopicsMap.get(industryId);
	            if(null!=oldTopics&&oldTopics.size()>0){
	            	hotTopics = HotTopicService.getTopN(oldTopics, hotTopics, decay, topN);
	            	
	            }
	            
	            oldTopics =new ArrayList<HotTopic>(hotTopics);
	            
		        oldTopicsMap.put(industryId, oldTopics);
		        
		        int i=0;
		        for(HotTopic hotTopic:hotTopics){
		        	if(hotTopic.getHotValue()>1){
		        		System.out.println("industryId:"+industryId+",mid:"+hotTopic.getMid()+",hotValue:"+hotTopic.getHotValue()+",increaseValue:"+hotTopic.getIncreaseValue());
		        	    i++;
		        	    if(i>2){
		        	    	break;
		        	    }
		        	}	        	
		        }
		        HotTopicService.saveHotTopicToHbase(industryId,hotTopics);
			}
			
			CheckpointUtil.persist("topics", oldTopicsMap);
			
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
