package com.cloudera.vms.similarity.policy;

import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;

public interface VotePolicy {
	public SimilarityGroup vote(SimilarityMember newMember, SimilarityGroup group);
}
