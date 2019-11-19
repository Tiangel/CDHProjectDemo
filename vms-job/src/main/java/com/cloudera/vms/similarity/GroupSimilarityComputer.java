package com.cloudera.vms.similarity;

import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;
import com.cloudera.vms.similarity.listener.SimilarityListener;
import com.cloudera.vms.similarity.policy.VotePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class GroupSimilarityComputer extends SimilarityComputer{
	Logger logger = LoggerFactory.getLogger(getClass());
	public SimilarityGroup compute(SimilarityGroup g, Map<String, SimilarityGroup> groups) {
		long l = System.currentTimeMillis();
		logger.info("[sim-job] g -> {}", g);
		SimilarityMember member = g.getMembers().stream().filter(m -> m.getHmCode().equals(g.getMeta())).findFirst().get();;
		SimilarityGroup group = computeAndMerge(g, groups, member);
		
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
	
	private SimilarityGroup computeAndMerge(SimilarityGroup newGroup, Map<String, SimilarityGroup> cachedGroups,
			SimilarityMember member) {
		Optional<SimilarityGroup> first = cachedGroups.values().parallelStream().filter(cachedGroup -> isSimilarity(newGroup.getMeta(), cachedGroup.getMeta())).findFirst();
		SimilarityGroup group ;
		if(first.isPresent()) {
			group = first.get();
		}else {
			group = newGroup;
			String groupCode = generateSimilarityCode(member);
			group.setCode(groupCode);
			group.setCreateDate(new Date());
		}
		group.getMembers().addAll(newGroup.getMembers());
		group.setMeta(member.getHmCode());
		return group;
	}

	@Override
	public GroupSimilarityComputer votePolicy(VotePolicy votePolicy) {
		super.votePolicy(votePolicy);
		return this;
	}

	@Override
	public GroupSimilarityComputer success(SimilarityListener<SimilarityMember, SimilarityGroup> listener) {
		super.success(listener);
		return this;
	}
	
}
