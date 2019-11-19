package com.cloudera.vms.similarity.alert;

import java.util.Arrays;

import com.alibaba.fastjson.JSON;

/**
 * 预警触发条件
 * 
 * @author lijl@izhonghong.com
 *
 */
public class HotTopicCond{
	private String id;
	private String orgId;// 组织id
	private String type;// 类型 
	private int[] levels;// 分级
	private int period;// 时间范围
	private String articleType;// 媒体类型

	public String getArticleType() {
		return articleType;
	}

	public void setArticleType(String articleType) {
		this.articleType = articleType;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getOrgId() {
		return orgId;
	}

	public void setOrgId(String orgId) {
		this.orgId = orgId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int[] getLevels() {
		return levels;
	}

	public void setLevels(int[] levels) {
		this.levels = levels;
	}

	public int getPeriod() {
		return period;
	}

	public void setPeriod(int period) {
		this.period = period;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		HotTopicCond other = (HotTopicCond) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "AlertTriggerCond [id=" + id + ", orgId=" + orgId + ", type=" + type + ", levels="
				+ Arrays.toString(levels) + ", period=" + period + "]";
	}
	public static void main(String[] args) {
		HotTopicCond cond = new HotTopicCond();
		cond.setId("test");
		cond.setLevels(new int[] {2,4,6});
		cond.setOrgId("10006");
		cond.setPeriod(24);
		cond.setType("count");
		System.out.println(JSON.toJSONString(cond));
	}
}
