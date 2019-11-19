package com.cloudera.vms.similarity.policy;

import com.cloudera.vms.similarity.bean.SimilarityMember;

public interface SimilarityCodePolicy{
	public String generate(SimilarityMember member);
}
