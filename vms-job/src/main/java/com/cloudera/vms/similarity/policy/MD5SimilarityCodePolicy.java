package com.cloudera.vms.similarity.policy;

import org.apache.commons.codec.digest.DigestUtils;

import com.cloudera.vms.similarity.bean.SimilarityMember;

public class MD5SimilarityCodePolicy implements SimilarityCodePolicy {

	@Override
	public String generate(SimilarityMember member) {
		return DigestUtils.md5Hex(member.getHmCode() + System.currentTimeMillis());
	}

}
