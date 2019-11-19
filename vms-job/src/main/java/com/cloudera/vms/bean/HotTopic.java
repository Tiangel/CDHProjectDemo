package com.cloudera.vms.bean;

import java.io.Serializable;
import java.util.List;



public class HotTopic implements Comparable<HotTopic> , Serializable{
	
	private String mid;
	private Long hanmingCode;
	private Double hotValue;
	private Double increaseValue;
	private String msg;
	private List<String> topicSamples;

	
	public String getMid() {
		return mid;
	}
	public void setMid(String mid) {
		this.mid = mid;
	}
	
	
	
	public Double getHotValue() {
		return hotValue;
	}
	public void setHotValue(Double hotValue) {
		this.hotValue = hotValue;
	}
	public Double getIncreaseValue() {
		return increaseValue;
	}
	public void setIncreaseValue(Double increaseValue) {
		this.increaseValue = increaseValue;
	}
	public Long getHanmingCode() {
		return hanmingCode;
	}
	public void setHanmingCode(Long hanmingCode) {
		this.hanmingCode = hanmingCode;
	}
	public int compareTo(HotTopic o) {
		return -(this.hotValue-o.hotValue>0?1:(this.hotValue-o.hotValue==0?0:-1));
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public List<String> getTopicSamples() {
		return topicSamples;
	}
	public void setTopicSamples(List<String> topicSamples) {
		this.topicSamples = topicSamples;
	}
	
	


}
