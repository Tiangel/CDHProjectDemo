package com.cloudera.vms.similarity.bean;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;
/**
 * 相似组
 * @author lijl@izhonghong.com
 *
 */
public class SimilarityGroup implements Serializable{
	
	private static final long serialVersionUID = 1L;
	protected String code;//相似码
	protected String meta;// 元 相似计算使用的hmcode
	protected Set<SimilarityMember> members;
	protected Date createDate;
	
	
	public String getMeta() {
		return meta;
	}


	public void setMeta(String meta) {
		this.meta = meta;
	}


	public Set<SimilarityMember> getMembers() {
		return members;
	}


	public void setMembers(Set<SimilarityMember> members) {
		this.members = members;
	}


	public Date getCreateDate() {
		return createDate;
	}


	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}


	public void setCode(String code) {
		this.code = code;
	}


	public String getCode() {
		return code;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((code == null) ? 0 : code.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimilarityGroup other = (SimilarityGroup) obj;
		if (code == null) {
			if (other.code != null)
				return false;
		} else if (!code.equals(other.code))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "SimilarityGroup [code=" + code + ", meta=" + meta + ", members=" + members + ", createDate="
				+ createDate + "]";
	}
	
}
