package com.cloudera.vms.service;
import com.cloudera.vms.Config;
import com.cloudera.vms.bean.HotTopic;
import com.cloudera.vms.hbase.HBaseJavaAPI;
import com.cloudera.vms.hbase.RowBean;
import com.cloudera.vms.utils.TFIDF;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class HotTopicService {
	

	public static Set<String> getHotIds(List<HotTopic> oldTopics){
		Set<String> mids = new HashSet<String>();
		for(HotTopic oldTopic:oldTopics){
			mids.addAll(oldTopic.getTopicSamples());
		}
		return mids;
	}
	
	public static List<HotTopic> getTopN(List<HotTopic> oldTopics,List<HotTopic> newTopics,double  r,int N){
		Set<String> hotMids = getHotIds(oldTopics);
		Map<String,Double> oldHotValues = new HashMap<String,Double>();
		Map<String,Double> newHotValues = new HashMap<String,Double>();
		List<Tuple2<String,Long>> midHanmings = new ArrayList<Tuple2<String,Long>>();
		List<List<Short>> groups = new ArrayList<List<Short>>();
		Map<String,HotTopic> oldTopicMap = new HashMap<String,HotTopic>();
		Map<String,HotTopic> newTopicMap = new HashMap<String,HotTopic>();
		for(HotTopic topic:oldTopics){
			Double hotValue = topic.getHotValue();
			int samples = topic.getTopicSamples().size();
			if(hotValue<samples&&samples<10){
				topic.setHotValue((double)samples);
			}
			oldHotValues.put(topic.getMid(), topic.getHotValue());
			oldTopicMap.put(topic.getMid(), topic);
			Tuple2<String,Long> midHanming = new Tuple2(topic.getMid(),topic.getHanmingCode());
			midHanmings.add(midHanming);
		}
		for(HotTopic topic:newTopics){
			if(hotMids.contains(topic.getMid())){
				continue;
			}
			newTopicMap.put(topic.getMid(), topic);
			newHotValues.put(topic.getMid(), topic.getHotValue());
			Tuple2<String,Long> midHanming = new Tuple2(topic.getMid(),topic.getHanmingCode());
			midHanmings.add(midHanming);
		}
		
		Set<Short> grouped = new HashSet<Short>();
        for(int k=0;k<midHanmings.size();k++){
        	if(grouped.contains((short) k)){
    			continue;
    		}else{
    			grouped.add((short) k);
    		}
        	List<Short> group = new ArrayList<Short>();
        	group.add((short)k);
        	groups.add(group);
        
        	for(int l=k+1;l<midHanmings.size();l++){
        		if(grouped.contains((short) l)){
        			continue;
        		}
        		int distance = TFIDF.getHanmingDistance(midHanmings.get(k)._2, midHanmings.get(l)._2);
        		if(distance<Integer.parseInt(Config.get(Config.KEY_CLUSTING_GROUP_GAP))){
        			group.add((short)l);
        			grouped.add((short)l);
        		}
        		
        	}
        
        }
        List<HotTopic> topics = new ArrayList<HotTopic>();
        
        for(List<Short> group:groups){
        	HotTopic hotTopic = new HotTopic();
        	int increaseValue= 0;
        	double hotValue = 0;
        	for(Short i:group){
        		String mid = midHanmings.get(i)._1;
        		Double oldHotValue = oldHotValues.get(mid);
        		if(null!=oldHotValue){
        			hotValue+=new BigDecimal(oldHotValue*r).setScale(4, RoundingMode.UP).doubleValue();
        		}
        		Double newHotValue = newHotValues.get(mid);
        		if(null!=newHotValue){
        			hotValue += newHotValue;
        			increaseValue += newHotValue;
        		}
        		
        	}
        	hotTopic.setIncreaseValue((double)increaseValue);
        	hotTopic.setHotValue(hotValue);
        	hotTopic.setHanmingCode(midHanmings.get(group.get(0))._2);
        	hotTopic.setMid(midHanmings.get(group.get(0))._1);
        	List<String> mids = new ArrayList<String>();
        	for(Short i:group){
        		String mid = midHanmings.get(i)._1;
        		HotTopic oldTopic = oldTopicMap.get(mid);
        		if(null!=oldTopic){
        			List<String> oldMids = oldTopic.getTopicSamples();
        			for(String oldMid:oldMids){
        				if(!mids.contains(oldMid)){
        					if(mids.size()>=10){
        						mids.remove(0);      						
        					}
        					mids.add(oldMid);
        					
        				}
        				
        			}
        		}else{
        			HotTopic newTopic =newTopicMap.get(mid);
        			if(null!=newTopic){
        				List<String> ids =newTopic.getTopicSamples();
        				for(String id:ids){
        					if(!mids.contains(id)){
            					if(mids.size()>=10){
            						mids.remove(0);      						
            					}
            					mids.add(id);
            					
            				}
        				}
        				
        			}
        		}
        		
        		if(!mids.contains(mid)){
        			if(mids.size()>=10){
						mids.remove(0);		
					}
        			mids.add(mid);
        		}
        		
        	}
        	hotTopic.setTopicSamples(mids);
        	
        	topics.add(hotTopic);
        }
        Collections.sort(topics);
        if(N<topics.size()){
        	return topics.subList(0, N);
        }
		return topics;

	}
	

	
	public static List<HotTopic> getTopN(List<Tuple2<String,Long>> hanmings,int N){
		List<Tuple2<String,Long>> validHanmings = new ArrayList<Tuple2<String,Long>>();
		for(Tuple2<String,Long> hanming:hanmings){
			if(hanming._2!=null&&TFIDF.notZoreBitCount(hanming._2)>9&&TFIDF.notZoreBitCount(hanming._2)<61){
				validHanmings.add(hanming);
			}
		}
		hanmings = validHanmings;
		List<List<Short>> groups = new ArrayList<List<Short>>();
		Set<Short> grouped = new HashSet<Short>();
        for(int k=0;k<hanmings.size();k++){
        	if(grouped.contains((short) k)){
    			continue;
    		}else{
    			grouped.add((short) k);
    		}
        	List<Short> group = new ArrayList<Short>();
        	group.add((short)k);
        	groups.add(group);
      
        	for(int l=k+1;l<hanmings.size();l++){
        		
        		if(grouped.contains((short) l)){
        			continue;
        		}
        		int distance = TFIDF.getHanmingDistance(hanmings.get(k)._2, hanmings.get(l)._2);
        		if(distance<Integer.parseInt(Config.get(Config.KEY_CLUSTING_GROUP_GAP))){
        			group.add((short)l);
        			grouped.add((short)l);
        		}
        		
        	}
        
        }
        List<HotTopic> topics = new ArrayList<HotTopic>();
        
        for(List<Short> group:groups){
        	HotTopic hotTopic = new HotTopic();
        	hotTopic.setIncreaseValue((double)group.size());
        	hotTopic.setHotValue((double)group.size());
        	hotTopic.setHanmingCode(hanmings.get(group.get(0))._2);
        	hotTopic.setMid(hanmings.get(group.get(0))._1);
        	List<String> mids = new ArrayList<String>();
        	for(Short i:group){
        		String mid = hanmings.get(i)._1;
        		if(!mids.contains(mid)){
        			if(mids.size()>=10){
						mids.remove(0);      						
					}
					mids.add(mid);
        		}else{
        		 double hotValue = hotTopic.getHotValue();
        		 if(hotValue>1){
        			 hotValue--;
        			 hotTopic.setHotValue(hotValue);
        		 }
        		}
        	}
        	hotTopic.setTopicSamples(mids);
        	topics.add(hotTopic);
        }
        Collections.sort(topics);
        if(N<topics.size()){
        	return topics.subList(0, N);
        }
		return topics;
	}
	
	public static void saveHotTopicToHbase(String type,List<HotTopic> hotTopics){
		List<RowBean> rowBeans = new ArrayList<RowBean>();
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String rowKey = type+"_"+df.format(Calendar.getInstance().getTime());
		System.out.println("rowKey:"+rowKey);
		for(int i=0;i<hotTopics.size();i++){
			HotTopic hotTopic = hotTopics.get(i);
			RowBean row = new RowBean();
			row.setColumn("rank"+i);
			row.setRow(rowKey);
			row.setColumnFamily("ranks");
			List<String> mids = hotTopic.getTopicSamples();
			StringBuilder topicSamples = new StringBuilder();
			for(String mid:mids){
				if(topicSamples.length()>0){
					topicSamples.append("|");
				}
				topicSamples.append(mid);

			}
			String value = hotTopic.getMid()+","+hotTopic.getHotValue().intValue()+","+hotTopic.getIncreaseValue().intValue()+","+topicSamples;
			row.setValue(value);
			rowBeans.add(row);
		}
		HBaseJavaAPI.addRows(Config.get(Config.KEY_HBASE_HOTTOPIC_NAME), rowBeans);
	}

}
