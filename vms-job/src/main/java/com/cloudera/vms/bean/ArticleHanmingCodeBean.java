package com.cloudera.vms.bean;



import java.io.Serializable;
import java.sql.Timestamp;


public class ArticleHanmingCodeBean implements Serializable {

    /**
	 * 
	 */
//	private static final long serialVersionUID = 1L;

	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	private String mid; // 微博消息id，文档唯一标示.

	private String text; // 信息内容.

	private Timestamp created_at; // 文章发布时间
	private String crawler_time;

	public String getCrawler_time() {
		return crawler_time;
	}

	public void setCrawler_time(String crawler_time) {
		this.crawler_time = crawler_time;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Timestamp getCreated_at() {
		return created_at;
	}

	public void setCreated_at(Timestamp created_at) {
		this.created_at = created_at;
	}
	public static void main(String[] args) {
		Timestamp stampe = new Timestamp(System.currentTimeMillis());
		System.out.println(String.valueOf(stampe));
	}
}
