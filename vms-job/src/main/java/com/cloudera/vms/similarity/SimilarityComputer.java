package com.cloudera.vms.similarity;

import com.alibaba.fastjson.JSON;
import com.cloudera.vms.Config;
import com.cloudera.vms.similarity.alert.HotTopicCond;
import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;
import com.cloudera.vms.similarity.filter.SimilarityFilter;
import com.cloudera.vms.similarity.listener.SimilarityListener;
import com.cloudera.vms.similarity.policy.MD5SimilarityCodePolicy;
import com.cloudera.vms.similarity.policy.SimilarityCodePolicy;
import com.cloudera.vms.similarity.policy.VotePolicy;
import com.cloudera.vms.utils.HanmingCode;
import com.cloudera.vms.utils.JedisClusterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SimilarityComputer {

	private static final double DEFAULT_FACTOR = 0.7;
	private static Logger logger = LoggerFactory.getLogger(SimilarityComputer.class);
	protected VotePolicy votePolicy;// 组内选举策略
	private SimilarityCodePolicy similarityCodePolicy = new MD5SimilarityCodePolicy();// similarityCode生成策略
	private List<SimilarityFilter<SimilarityMember>> filters = new ArrayList<>();
	private List<SimilarityListener<SimilarityMember, SimilarityGroup>> successListeners = new ArrayList<>();

	public static final String REDIS_KEY_SIM_GROUP_META = Config.get("redis.sim.group-meta");// code - meta
	public static final String REDIS_KEY_SIM_GROUP = Config.get("redis.sim.group");// code

	public static final String REDIS_KEY_SIM_GROUP_FLAG_PREFIX = "{sim-group}";// 逾期标记前缀

	JedisCluster jedis = JedisClusterUtils.getJedisCluster();// 集群模式

	public static boolean isSimilarity(String code1, String code2, double factor) {

		if (code1 == null || code2 == null)
			return false;
		byte[] bytes1 = HanmingCode.decode(code1);
		byte[] bytes2 = HanmingCode.decode(code2);
		boolean similirity = isSimilarity(bytes1, bytes2, factor);
		// if(similirity)
		// logger.info("[sim computer] hmCode1 -> {}, hmCode2 -> {}", code1, code2);
		return similirity;
	}

	public static boolean isSimilarity(String code1, String code2) {
		return isSimilarity(code1, code2, DEFAULT_FACTOR);
	}

	public static boolean isSimilarity(byte[] bytes1, byte[] bytes2, double factor) {
		if (bytes1 == null || bytes2 == null)
			return false;

		double jaccardCoefficient = getJaccardCoefficient(bytes1, bytes2);
		return jaccardCoefficient >= factor;
	}

	public static boolean isSimilarity(byte[] bytes1, byte[] bytes2) {
		return isSimilarity(bytes1, bytes2, DEFAULT_FACTOR);
	}

	/**
	 * 计算jaccard 相似系数
	 * 
	 * @param code1
	 * @param code2
	 * @return
	 */
	public static double getJaccardCoefficient(String code1, String code2) {
		if (code1 == null || code2 == null)
			return 0;
		byte[] bytes1 = HanmingCode.decode(code1);
		byte[] bytes2 = HanmingCode.decode(code2);

		return getJaccardCoefficient(bytes1, bytes2);

	}

	public static double getJaccardCoefficient(byte[] bytes1, byte[] bytes2) {
		if (bytes1 == null || bytes2 == null)
			return 0;

		short intersection = getFeatureIntersection(bytes1, bytes2);
		short union = getFeatureUnion(bytes1, bytes2);
		return new BigDecimal(intersection).divide(new BigDecimal(union), 4, RoundingMode.HALF_UP).doubleValue();

	}

	/**
	 * 特征计数
	 * 
	 * @param a
	 * @return
	 */
	public static short countFeature(byte[] a) {
		short count = 0;
		for (int j = 0; j < a.length; j++) {
			byte t = a[j];
			for (int i = 0; i < 8; i++) {

				count += t & 1;
				t = (byte) (t >> 1);
			}
		}

		return count;
	}

	/**
	 * 获取特征交集
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static short getFeatureIntersection(byte[] a, byte[] b) {
		short common = 0;
		for (int i = 0; i < a.length; i++) {
			byte c = (byte) (a[i] & b[i]);// 与运算

			for (int j = 0; j < 8; j++) {
				common += c & 1;// 与运算
				c = (byte) (c >> 1);// 右移1位(除以2)
			}
		}

		return common;

	}

	/**
	 * 获取特征并集
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static short getFeatureUnion(byte[] a, byte[] b) {
		short common = 0;
		for (int i = 0; i < a.length; i++) {
			byte c = (byte) (a[i] | b[i]);

			for (int j = 0; j < 8; j++) {
				common += c & 1;
				c = (byte) (c >> 1);
			}
		}

		return common;

	}

	static int TIME_LIMIT = 3;

	/**
	 * 计算相似值
	 * 
	 * @param string
	 * @return
	 */
	public SimilarityGroup compute(String string, String hmCode, Map<String, SimilarityGroup> groups) {
		long l = System.currentTimeMillis();
		logger.info("[sim-compute] start compute similarity...");
		SimilarityMember member = JSON.parseObject(string, SimilarityMember.class);
		member.setDocStr(string);
		member.setHmCode(hmCode);

		if (filter(member, null)) {
			logger.info("[sim-compute] member illigale... member -> {}", member.getMid());
			return null;
		}
		logger.info("[sim-compute] step 1 -> filter member, member -> {}, cost -> {}", member.getMid(),
				System.currentTimeMillis() - l);

		SimilarityGroup group;
		if (groups == null) {
			group = computeAndMerge(member);
		} else {
			group = computeAndMerge(member, groups);
		}
		logger.info("[sim-compute] step 2 -> get or create group, member -> {}, cost -> {}", member.getMid(),
				System.currentTimeMillis() - l);

		success(member, group);// 触发成功事件
		logger.info("[sim-compute] step 3 -> deal success, member -> {}, cost -> {}", member.getMid(),
				System.currentTimeMillis() - l);

		String oriMeta = group.getMeta();
		voteInGroup(member, group, votePolicy);// 选举新meta
		logger.info("[sim-compute] step 4 -> vote in group, member -> {}, cost -> {}", member.getMid(),
				System.currentTimeMillis() - l);

		updateOrInsertGroup(group, oriMeta);// 更新或插入新组
		logger.info("[sim-compute] step 5 -> save group, member -> {}, group -> {}, cost -> {}", member.getMid(),
				group.getCode(), System.currentTimeMillis() - l);

		logger.info("[sim-compute] finish compute similarity...");
		return group;
	}


	protected void updateOrInsertGroup(SimilarityGroup group, String oriMeta) {
		logger.info("[sim-group] update group -> {}", group);

		String code = group.getCode();
		jedis.hset(REDIS_KEY_SIM_GROUP_META, code, group.getMeta());
		jedis.hset(REDIS_KEY_SIM_GROUP, code, JSON.toJSONString(group));

		jedis.set(REDIS_KEY_SIM_GROUP_FLAG_PREFIX + code, "");
		jedis.expire(REDIS_KEY_SIM_GROUP_FLAG_PREFIX + code, 60 * 60 * 24 * 3);

	}

	private SimilarityGroup computeAndMerge(SimilarityMember member) {
		String cursor = "0";

		String groupKey = null;
		a: while (true) {

			ScanResult<Entry<String, String>> result = jedis.hscan(REDIS_KEY_SIM_GROUP_META, cursor);
			cursor = result.getStringCursor();

			List<Entry<String, String>> entrys = result.getResult();// keys
			if (entrys == null)
				continue;
			for (Entry<String, String> entry : entrys) {
				String code = entry.getKey();
				String value = entry.getValue();
				boolean similirity = isSimilarity(member.getHmCode(), value);
				if (similirity) {
					groupKey = code;
					break a;
				}
			}
			if ("0".equals(cursor))
				break;
		}
		logger.info("[sim-job] mid -> {}, group key -> {}", member.getMid(), groupKey);
		if (groupKey == null)
			return newGroup(member);

		SimilarityGroup group = getGroup(groupKey);
		group.getMembers().add(member);
		logger.info("[sim-group] find sim-group -> {}", group.getCode());
		return group;
	}

	/**
	 * 计算并合并分组信息
	 * @param member
	 * @param groups key
	 * @return
	 */
	private SimilarityGroup computeAndMerge(SimilarityMember member, Map<String, SimilarityGroup> groups) {
		SimilarityGroup group ;
		Optional<Entry<String, SimilarityGroup>> findFirst = groups.entrySet().stream().filter(e -> {
			SimilarityGroup tmp = e.getValue();
			return  isSimilarity(member.getHmCode(), tmp.getMeta());
		}).findFirst();
		
		if(findFirst != null && findFirst.isPresent()) {
			group = findFirst.get().getValue();
		}else {
			group = newGroup(member);
		}
		
		if (!groups.containsValue(group))
			groups.put(group.getCode(), group);
		
		Set<SimilarityMember> members = group.getMembers();
		if( ! members.contains(member)) members.add(member);
		
		return group;
	}
	
	private static void removeExpiredCode(List<String> couldRemove) {
		JedisCluster jedis = JedisClusterUtils.getJedisCluster();
		if (couldRemove == null || couldRemove.isEmpty())
			return;

		couldRemove.parallelStream().forEach(code -> {

//			logger.info("[sim-job] remove expired hkey -> {}", code);
			jedis.hdel(REDIS_KEY_SIM_GROUP_META, code);
			jedis.hdel(REDIS_KEY_SIM_GROUP, code);

		});


	}

	private SimilarityGroup getGroup(String groupCode) {
		logger.info("[sim-compute] get group , group -> {}", groupCode);
		String groupJsonStr = jedis.hget(REDIS_KEY_SIM_GROUP, groupCode);
		// String groupJsonStr = jedis.get(groupCode);
		return JSON.parseObject(groupJsonStr, SimilarityGroup.class);

	}

	private SimilarityGroup newGroup(SimilarityMember member) {
		logger.info("[sim-compute] new group , member -> {}", member.getMid());
		SimilarityGroup group = new SimilarityGroup();
		String groupCode = generateSimilarityCode(member);
		group.setCode(groupCode);
		group.setCreateDate(new Date());
		group.setMembers(new HashSet<>(Arrays.asList(member)));
		group.setMeta(member.getHmCode());
		return group;
	}

	public static Map<String, SimilarityGroup> getGroups() {
		JedisCluster jedis = JedisClusterUtils.getJedisCluster();
		String cursor = "0";
		Map<String, SimilarityGroup> groups = new ConcurrentHashMap<>(100000);
		List<String> expiredKeys = new ArrayList<>();
		while (true) {

			ScanResult<Entry<String, String>> result = jedis.hscan(SimilarityComputer.REDIS_KEY_SIM_GROUP, cursor);
			cursor = result.getStringCursor();

			List<Entry<String, String>> entrys = result.getResult();// keys
			if (entrys == null)
				continue;
			entrys.parallelStream().forEach(entry -> {
				String code = entry.getKey();
				String value = entry.getValue();

				String val = jedis.get(REDIS_KEY_SIM_GROUP_FLAG_PREFIX + code);
				if (val == null) {
					expiredKeys.add(code);
				} else
					try {
						groups.put(code, JSON.parseObject(value, SimilarityGroup.class));
					} catch (Exception e) {
						logger.error("parse sim group failed ... vaklue -> {}", value, e);
					}
			});
			if ("0".equals(cursor))
				break;
		}
		logger.info("[sim-job] groups size -> {}", groups.size());
		removeExpiredCode(expiredKeys);
		return groups;

	}

	protected String generateSimilarityCode(SimilarityMember member) {
		return similarityCodePolicy.generate(member);
	}

	protected void voteInGroup(SimilarityMember newMember, SimilarityGroup group, VotePolicy policy) {
		if (policy == null)
			return;
		policy.vote(newMember, group);
	}

	private boolean filter(SimilarityMember member, List<HotTopicCond> conds) {
		return filters.stream().anyMatch(filter -> filter.filter(member, conds));
	}

	protected void success(SimilarityMember member, SimilarityGroup group) {
		successListeners.forEach(listener -> listener.trigger(member, group));
	}

	public void setSimilarityCodePolicy(SimilarityCodePolicy similarityCodePolicy) {
		this.similarityCodePolicy = similarityCodePolicy;
	}

	public SimilarityComputer votePolicy(VotePolicy votePolicy) {
		this.votePolicy = votePolicy;
		return this;
	}

	public SimilarityComputer filter(SimilarityFilter<SimilarityMember> filter) {
		filters.add(filter);
		return this;
	}

	public SimilarityComputer success(SimilarityListener<SimilarityMember, SimilarityGroup> listener) {

		successListeners.add(listener);
		return this;
	}

	public static void main(String[] args) {
		//22b846351bff3b0786b586ba56d3dcb4
		String group1 = "0xe60xb20x8f0xe60xa10x8b0xe60x850x900xe60x800x9e0xe50xab0xb30xe40xb80xb20xe70xa40x830xe60xb80x930xe50xaf0x900xe50xa20x8c0xe50xbb0x810xe40xb90xb80xe70x980xb00xe50x860x9a0xe70x840x920xe80x9a0x840xe70x820xa00xe70x880x860xe70x880x880xe60xbb0x880xe50x860x910xe70x830x800xe70xb20x980xe50x880x800xe70xac0x960xe80x990xa90xe50x8c0x950xe60x820xa60xe80x8c0x8f0xe60xb80xa40xe60x8a0x9d0xe60x980xa3";
		
		//097adb35e682610d459eedbbbfaef151
		String group2 = "0xe60xb20x8f0xe60xa10x8b0xe60x810x900xe60x820x9e0xe50x9b0xb30xe40xb80xb20xe70xa40x830xe60xb90x930xe50xaf0x940xe50xa30xae0xe70xbb0x810xe40xb90xb80xe60xb80xb00xe50x860x9a0xe70x840x920xe80x9a0x840xe70x820xa00xe70x880x8e0xe70x880x880xe70x870x880xe50x850x910xe70x830x840xe70x920x980xe50xa80x800xe70xac0x960xe80x990xa90xe50x8c0x950xe60x820xa60xe80x8c0x8f0xe60xb80xa40xe60x8a0xbd0xe60xa40xa3";
		
		String hmCode1 = toString(group1);
		String hmCode2 = toString(group2);
		System.out.println(isSimilarity("朕腗椑撅萡备焮匋箚寔虰羽佐杬渁笆懆庢讜劤坡欚繴彰弬蘪掘暊贑螌汲渣", "歵蠗櫕斝聁崮璾宁嚞圌坷暻埜睝皱窆薦嚺讎谧坱赟菀孢弙詂愘藆賤诃謯熱"));

	}
	private static String toString(String code) {
		String tmp = "";
		List<Byte> list = new LinkedList<>();
		for(int i = 0; i < code.length(); i ++) {
			tmp += code.charAt(i);
			if(tmp.length() == 4) {
				list.add((byte) Integer.parseInt(tmp.replace("0x", ""), 16));
				tmp = "";
			}
		}
		System.out.println(list);
		byte[] bytes = new byte[list.size()];
		for(int i = 0 ; i< bytes.length; i ++) {
			bytes[i] = list.get(i);
			
		}
		System.out.println(new String(bytes));
		return new String(bytes);
	}
}
