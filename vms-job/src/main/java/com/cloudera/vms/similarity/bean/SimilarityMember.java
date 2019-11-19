package com.cloudera.vms.similarity.bean;

import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * redis中缓存数据<br>
 * ps: 如果数据量太大, 相当于把全部的文档都缓存一遍 效率
 * 
 * @author lijl@izhonghong.com
 *
 */
public class SimilarityMember implements Serializable{
	private static final long serialVersionUID = 1L;
	protected String hmCode;
	@JSONField(name = "created_at", format = "yyyy-MM-dd HH:mm:ss.SSS")
	protected Date createDate;
	protected String mid;
	@JSONField(name = "orgs_tag")
	protected String monitorOrgs;// 命中监控的组织
	@JSONField(name = "events_tag")
	protected String eventsTag;// 命中的专题
	@JSONField(serialize = false)
	protected String docStr;// 整个文章 不做缓存
	protected String code;
	@JSONField(serialize = false)
	protected String createdDateStr;
	
	public String getCreatedDateStr() {
		return createdDateStr;
	}

	public void setCreatedDateStr(String createdDateStr) {
		this.createdDateStr = createdDateStr;
	}

	public String getEventsTag() {
		return eventsTag;
	}

	public void setEventsTag(String eventsTag) {
		this.eventsTag = eventsTag;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getHmCode() {
		return hmCode;
	}

	public void setHmCode(String hmCode) {
		this.hmCode = hmCode;
	}

	public String getDocStr() {
		return docStr;
	}

	public void setDocStr(String docStr) {
		this.docStr = docStr;
	}

	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}
	

	public String getMonitorOrgs() {
		return monitorOrgs;
	}

	public void setMonitorOrgs(String monitorOrgs) {
		this.monitorOrgs = monitorOrgs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mid == null) ? 0 : mid.hashCode());
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
		SimilarityMember other = (SimilarityMember) obj;
		if (mid == null) {
			if (other.mid != null)
				return false;
		} else if (!mid.equals(other.mid))
			return false;
		return true;
	}



	@Override
	public String toString() {
		return "SimilarityMember [hmCode=" + hmCode + ", createDate=" + createDate + ", mid=" + mid + ", monitorOrgs="
				+ monitorOrgs +  "]";
	}

	public static void main(String[] args) {
//		SimilarityGroup group = new SimilarityGroup();
		SimilarityMember member = new SimilarityMember();
//		member.setCreateDate(new Date());
//		member.setDocStr("xxxxxx");
//		member.setHmBytes("xxxx".getBytes());
//		member.setMid("mid");
//		member.setMonitorOrgs(Arrays.asList("11", "22"));
//		String hmStr = new String(member.getHmBytes());
//		String groupCode = "1112";
//		group.setCode(groupCode);
//		group.setCreateDate(new Date());
//		group.setMembers(Arrays.asList(member));
//		group.setMeta(hmStr);
//
//		System.out.println(JSON.toJSONString(group));
		String json = "{\"created_at\":\"2018-07-26 14:52:44.581\",\"hmBytes\":\"eHh4eA==\",\"mid\":\"mid\",\"orgs_tag\":\"11,23,45\"}";
		System.out.println(member.getMonitorOrgs());
	}
}
