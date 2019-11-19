package com.cloudera.vms.similarity.stay_focus;

import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 持续关注条件实体
 * 
 * @author lijl@izhonghong.com
 *
 */
public class StayFocusCond implements Serializable{
	private static final long serialVersionUID = 1L;
	String id;
	String title;
	@JSONField(name = "context")
	String text;
	String url;
	int organizationid;
	@JSONField(name = "starttime", format = "yyyy-MM-dd")
	Date startTime;
	@JSONField(name = "endtime", format = "yyyy-MM-dd")
	Date eneTime;
	String hmCode;
	public String getHmCode() {
		return hmCode;
	}

	public void setHmCode(String hmCode) {
		this.hmCode = hmCode;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getOrganizationid() {
		return organizationid;
	}

	public void setOrganizationid(int organizationid) {
		this.organizationid = organizationid;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEneTime() {
		return eneTime;
	}

	public void setEneTime(Date eneTime) {
		this.eneTime = eneTime;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
}
