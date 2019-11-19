package com.cloudera.vms.similarity.filter;

import java.util.List;

import com.cloudera.vms.similarity.alert.AlertTrigger;
import com.cloudera.vms.similarity.alert.HotTopicCond;
import com.cloudera.vms.similarity.bean.SimilarityMember;

public class HotTopicSimilarityFilter implements SimilarityFilter<SimilarityMember>{
	
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(SimilarityMember member, List<HotTopicCond> conds) {
		return ! AlertTrigger.instance().trigger(member, conds);
	}

}
