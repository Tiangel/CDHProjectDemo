package com.cloudera.vms.es;

import java.io.Serializable;





public class EsWeibo implements Serializable{

	private String id;
	private Long hanmingCode;
	
	public EsWeibo(String id,Long hanmingCode){
		this.id = id;
		this.hanmingCode = hanmingCode;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Long getHanmingCode() {
		return hanmingCode;
	}
	public void setHanmingCode(Long hanmingCode) {
		this.hanmingCode = hanmingCode;
	}
	
	
	
	public static void main(String[] args){
	}
	
	
}
