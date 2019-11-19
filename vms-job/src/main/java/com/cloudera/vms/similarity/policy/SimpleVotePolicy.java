package com.cloudera.vms.similarity.policy;

import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;

public class SimpleVotePolicy implements VotePolicy {

	@Override
	public SimilarityGroup vote(SimilarityMember newMember, SimilarityGroup group) {
		group.setMeta(newMember.getHmCode());
		return group;
	}

}
