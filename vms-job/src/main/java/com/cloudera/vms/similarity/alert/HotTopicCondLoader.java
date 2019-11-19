package com.cloudera.vms.similarity.alert;

import com.alibaba.fastjson.JSON;
import com.cloudera.vms.Config;
import com.cloudera.vms.utils.JedisClusterUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class HotTopicCondLoader implements Serializable{

	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(HotTopicCondLoader.class);
	private static List<HotTopicCond> conds;
	private static final String COND_TYPE = Config.get("alert.cond.type.hot");

	public static List<HotTopicCond> conds() {
		if (conds == null) {
			synchronized (HotTopicCondLoader.class) {
				if (conds == null)
					refresh();
			}
		}

		return conds;

	}

	public static String REDIS_KEY_ALERT_COND = Config.get("redis.alert.cond");

	public static List<HotTopicCond> refresh() {
		logger.info("[htc loader] refresh...");
		JedisCluster jedis = JedisClusterUtils.getJedisCluster();
		List<HotTopicCond> tmp = new ArrayList<>();
		String cursor = "0";
		while (true) {

			ScanResult<Entry<String, String>> result = jedis.hscan(REDIS_KEY_ALERT_COND, cursor);
			cursor = result.getStringCursor();
			List<Entry<String, String>> entrys = result.getResult();

			if (entrys != null)
				entrys.forEach(e -> {
					HotTopicCond cond = JSON.parseObject(e.getValue(), HotTopicCond.class);
					if (StringUtils.equalsIgnoreCase(COND_TYPE, cond.getType()))
						tmp.add(cond);
				});

			if ("0".equals(cursor))
				break;
		}
		logger.info("[alert-trigger] conds -> {}", tmp);
		conds = tmp;
		return conds;
	}

	public static void main(String[] args) {
		HotTopicCond cond = new HotTopicCond();
		cond.setId("test");
		cond.setLevels(new int[] { 10, 20, 30 });
		cond.setOrgId("10006");
		cond.setPeriod(1);
		cond.setType("similarity");
		System.out.println(JSON.toJSONString(cond));

	}
}
