package com.cloudera.vms.similarity.listener;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.vms.Config;
import com.cloudera.vms.similarity.alert.AlertTrigger;
import com.cloudera.vms.similarity.alert.HotTopicCond;
import com.cloudera.vms.similarity.alert.HotTopicCondLoader;
import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;
import com.cloudera.vms.utils.KafkaUtils;

public class HotTopicSimilarityListener implements SimilarityListener<SimilarityMember, SimilarityGroup> {
	Logger logger = LoggerFactory.getLogger(getClass());
	

	@Override
	public void accept(SimilarityMember member, SimilarityGroup group) {
		AlertTrigger trigger = AlertTrigger.instance();
		logger.info("[hot topic listener] listener start...");
		trigger.trigger(member, HotTopicCondLoader.conds());
		List<HotTopicCond> conds = trigger.getTriggeredConds();
		if(conds == null ) {
			logger.error("[hot topic listener] no cond matched !, mid -> {}", member.getMid());
			return;
		}
			
		
		conds.forEach(cond -> dealAlert(cond, member, group));
		logger.info("[hot topic listener] listener end.....");
	}

	private void dealAlert(HotTopicCond cond, SimilarityMember member, SimilarityGroup group) {
		AtomicInteger counter = new AtomicInteger(1); 
		group.getMembers().forEach(m -> {
			if (AlertTrigger.trigger(m, cond))
				counter.incrementAndGet();
		});
		
		if(group.getMembers().contains(member)) counter.decrementAndGet();// 新生成的组 只有新生成的组员
		int level = AlertTrigger.rank(counter.get(), cond);
		if(level >= 0) {
			sendMsg(cond, member, level, counter.get(), group.getCode());
		}
		
	}

	private void sendMsg(HotTopicCond cond, SimilarityMember member, int level, int hitNum, String code) {
		JSONObject msg = new JSONObject();
		msg.put("cond", cond);
		JSONObject doc = JSON.parseObject(member.getDocStr());
		formatDoc(doc);
		msg.put("doc", doc);
		msg.put("hitLevel", level);
		msg.put("hitNum", hitNum);
		msg.put("id", code);
		String topic = Config.get("kafka.topic.alert.ams");
		logger.info("[alert-listener] msg -> {}, topic -> ", msg.toJSONString(), topic);
		KafkaUtils.syncSend(topic, msg.toJSONString());
	}
	String pattern = "yyyy-MM-dd HH:mm:ss";
	private void formatDoc(JSONObject doc) {
		Object createdAt = doc.get("created_at");
		if(createdAt != null) {
			if(createdAt instanceof Long) {
				DateTime time = new DateTime(((Long) createdAt).longValue());
				doc.put("created_at", time.toString(pattern));
			}
		}
		Integer media = doc.getInteger("article_type");
		
		if(media != null && media != 0) {
			String url = doc.getString("url");
			doc.put("article_url", url);
			doc.remove("url");
			doc.remove("weibo_url");
		}
		
		
	}
	public static void main(String[] args) {
		System.out.println(2 << 10);
	}
}
