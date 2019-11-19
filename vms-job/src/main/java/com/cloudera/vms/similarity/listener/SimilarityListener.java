package com.cloudera.vms.similarity.listener;

import com.cloudera.vms.similarity.bean.SimilarityGroup;
import com.cloudera.vms.similarity.bean.SimilarityMember;

import java.util.function.BiConsumer;
/**
 * 相似监听器
 * @author lijl@izhonghong.com
 *
 * @param <member>
 * @param <group>
 */
public interface SimilarityListener<member extends SimilarityMember, group extends SimilarityGroup> extends BiConsumer<member, group>{
	default public void trigger(member member, group group) {
		accept(member, group);
	}
}
