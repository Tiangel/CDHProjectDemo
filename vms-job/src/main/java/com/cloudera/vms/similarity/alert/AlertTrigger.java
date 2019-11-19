package com.cloudera.vms.similarity.alert;

import com.cloudera.vms.similarity.bean.SimilarityMember;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
/**
 * 非线程安全
 * @author lijl@izhonghong.com
 *
 */
public class AlertTrigger {
	
	Logger logger = LoggerFactory.getLogger(getClass());
	List<HotTopicCond> triggeredConds ;
	private AlertTrigger() {
		
	}
	
	public static boolean trigger(SimilarityMember member, HotTopicCond cond) {
		Objects.requireNonNull(member);
		Objects.requireNonNull(cond);
		return checkOrg(cond.getOrgId(), member) && checkPeriod(cond.getPeriod(), member);
	}


	public boolean trigger(SimilarityMember member, List<HotTopicCond> conds) {
		Objects.requireNonNull(member);
		triggeredConds = new ArrayList<>();
		if(conds == null ) conds = HotTopicCondLoader.conds();
		if(conds == null) return false;
		conds.forEach(cond -> {
			if(trigger(member, cond))
				triggeredConds.add(cond);
			
		});
		
		return ! triggeredConds.isEmpty();
	}

	private static boolean checkOrg(String orgId, SimilarityMember member) {
		Objects.requireNonNull(member);
		// 命中组织的监控专题
		String monitorOrgs = member.getMonitorOrgs();
		if(monitorOrgs == null) return false;
		
		boolean b = Arrays.stream(monitorOrgs.split(",")).anyMatch(str -> orgId.equals(str));
//		if( ! b)
//			logger.info("[alert-trigger] match org failed ! org -> {}, orgs -> {}", orgId, monitorOrgs);
		return b;
	}


	private static boolean checkPeriod(int period, SimilarityMember member) {
		Objects.requireNonNull(member);
		Date date = member.getCreateDate();
		return date != null && DateTime.now().minusDays(period).isBefore(date.getTime());
	}


	public List<HotTopicCond> getTriggeredConds() {
		return triggeredConds;
	}
	
	public static AlertTrigger instance() {
		return new AlertTrigger();
	}


	public static int rank(int num, HotTopicCond cond) {
		Objects.requireNonNull(cond);
		int[] levels = cond.getLevels();
		if(levels == null || levels.length == 0) return 0;
		int level = -1;
		for(int i = 0; i < levels.length ; i ++) {
			if(num >= levels[i]) level = i;
		}
		
		return level;
	}

	
}
