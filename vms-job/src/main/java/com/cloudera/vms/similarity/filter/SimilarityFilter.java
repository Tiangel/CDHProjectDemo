package com.cloudera.vms.similarity.filter;

import com.cloudera.vms.similarity.alert.HotTopicCond;
import com.cloudera.vms.similarity.bean.SimilarityMember;

import java.io.Serializable;
import java.util.List;

/**
 * 相似过滤
 * @author lijl@izhonghong.com
 *
 * @param <T>
 */
public interface SimilarityFilter<T extends SimilarityMember> extends Serializable{
	/**
	 * 
	 * @param member
	 * @param conds 
	 * @return true 需要过滤; false 不需要过滤
	 */
	public boolean filter(T member, List<HotTopicCond> conds);
}
