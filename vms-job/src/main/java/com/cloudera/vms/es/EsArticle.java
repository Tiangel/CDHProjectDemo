package com.cloudera.vms.es;

import java.io.Serializable;





public class EsArticle implements Serializable{

	private String id;
	private String hanmingCode;
	
	public EsArticle(String id,String hanmingCode){
		this.id = id;
		this.hanmingCode = hanmingCode;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getHanmingCode() {
		return hanmingCode;
	}
	public void setHanmingCode(String hanmingCode) {
		this.hanmingCode = hanmingCode;
	}
	
	
	
	public static void main(String[] args){
		EsArticle esWeibo = new EsArticle("1","");
	
	}
	
	
}
