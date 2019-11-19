package com.cloudera.vms.bean;



import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;


public class Article implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}
	private String key;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	private String mid; // 微博消息id，文档唯一标示.
	private String push_types;

	private String mid_f; // 首发微博消息id (站点Id_官网id).

	private String mid_p; // 转发的父微博的作者名
	private String events_tag;
	private String weiXinCode;

	private String source; // 微博来源,多个用','隔开.

	private Timestamp created_at; // 文章发布时间

	private String uid; // 作者id (站点Id_官网id).

	private String text_loc_country; // 微博国家（我们自己的地址编号）.

	private String text_loc_province; // 微博省份（我们自己的地址编号）.

	private String text_loc_city; // 微博城市（我们自己的地址编号）.

	private String emotion; // 正负面.

	private String text; // 信息内容.

	private String user_local; // 用户所在地（用户发微博时所在的地理位置）.

	private String text_subject; // 从属话题##.


	private Integer reposts_count; // 采集时的转发数.
	private Integer reports_count;//ms前端使用的转发数

	private Integer comments_count; // 评论数.
	
	private Integer read_count;//阅读数
	
	
	private Integer zan_count;//点赞数
    private Integer zans_count;//ams使用的点赞数
	private Integer grade_all; // 微博热度.

	private String pic_local; // 本地图片地址，多个图片用 “,” 号隔开.

	private String media_local; // 本地媒体类型:地址；多个用 “,” 号隔开.

	// private long score; // 热度汉明值.


	private String industries_tag; // 行业标签.

	private String name;//作者

	private String weibo_url;//

	private String text_loc;
	

	private List<String> tags;//订阅功能的客户自己的组织id
	private String updatetime;
	
    private String profileImageUrl;//作者头像
    
     private String sourceMid;//网站id_mid
     
    private String pic;//文章图片，用","分割多张
	//private List<String> pics;//文章图片数组
    
    private String created_date;
	
	private String title_top;
	
	private String address;//发布地点
	
	private String download_type;//下载渠道  0-99：微博,100-199:新闻APP,-1:IGET,200-299:微信
	
	private String verified_type;//文章作者认证类型,仅针对微博
	private String verifiedtype;//ams前端使用的
	private String crawler_site_id;//网站id 0:新浪微博，1：腾讯微博
	private String base_site_id;
	
	private String repost_from_site;//转发站点名
	
	private String url;//文章链接
	
	private String repost_from_url;//转发自的父文章url
	
	private String section;//板块
	
	private String crawler_time;//采集时间
	
	private Integer article_type;//0:微博,1:新闻app,2:微信,3:论坛,4:博客,5:报纸,6:视频,7:qq,8:跟帖,9:境外,10:twitter
	
	
	private String title;//标题
	
	private String summary;//摘要
	
	private int me;//for k3 //新闻（1）、论坛（2）、博客（3）、报纸（4）、视频（5）、微博（6）、微信（7）、QQ（8）、APP（9）、twitter（11）,跟帖（21）、境外媒体（-1）
	
	private String force;	//force=1强制更新
	
	private String history;//是否需要保存历史统计趋势
	
	private String biz;
	private String at_who;
	private Long crawlerDiffTime;//采集和发布时间差

	public Long getCrawlerDiffTime() {
		return crawlerDiffTime;
	}

	public void setCrawlerDiffTime(Long crawlerDiffTime) {
		this.crawlerDiffTime = crawlerDiffTime;
	}

	public String getAt_who() {
		return at_who;
	}

	public void setAt_who(String at_who) {
		this.at_who = at_who;
	}

	public String getPush_types() {
		return push_types;
	}

	public void setPush_types(String push_types) {
		this.push_types = push_types;
	}

	private Integer categoryId;

	public String getWeiXinCode() {
		return weiXinCode;
	}

	public void setWeiXinCode(String weiXinCode) {
		this.weiXinCode = weiXinCode;
	}

	public String getEvents_tag() {
		return events_tag;
	}

	public void setEvents_tag(String events_tag) {
		this.events_tag = events_tag;
	}

	public Integer getReports_count() {
		return reports_count;
	}

	public void setReports_count(Integer reports_count) {
		this.reports_count = reports_count;
	}

	public Integer getZans_count() {
		return zans_count;
	}

	public void setZans_count(Integer zans_count) {
		this.zans_count = zans_count;
	}

	public String getVerifiedtype() {
		return verifiedtype;
	}

	public void setVerifiedtype(String verifiedtype) {
		this.verifiedtype = verifiedtype;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public Integer getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(Integer categoryId) {
		this.categoryId = categoryId;
	}

	public String getBase_site_id() {
		return base_site_id;
	}

	public void setBase_site_id(String base_site_id) {
		this.base_site_id = base_site_id;
	}

	public String getBiz() {
		return biz;
	}

	public void setBiz(String biz) {
		this.biz = biz;
	}

	public String getHistory() {
		return history;
	}

	public void setHistory(String history) {
		this.history = history;
	}

	public String getForce() {
		return force;
	}

	public void setForce(String force) {
		this.force = force;
	}

	public int getMe() {
		return me;
	}

	public void setMe(int me) {
		this.me = me;
	}

	public Integer getReposts_count() {
		return reposts_count;
	}

	public Integer getComments_count() {
		return comments_count;
	}

	public Integer getZan_count() {
		return zan_count;
	}

	public Integer getGrade_all() {
		return grade_all;
	}

	public Integer getRead_count() {
		return read_count;
	}

	public void setRead_count(Integer read_count) {
		this.read_count = read_count;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public void setReposts_count(Integer reposts_count) {
		this.reposts_count = reposts_count;
	}

	public void setComments_count(Integer comments_count) {
		this.comments_count = comments_count;
	}

	public void setZan_count(Integer zan_count) {
		this.zan_count = zan_count;
	}

	public void setGrade_all(Integer grade_all) {
		this.grade_all = grade_all;
	}

	public Integer getArticle_type() {
		return article_type;
	}

	public void setArticle_type(Integer article_type) {
		this.article_type = article_type;
	}

	public String getCrawler_site_id() {
		return crawler_site_id;
	}

	public void setCrawler_site_id(String crawler_site_id) {
		this.crawler_site_id = crawler_site_id;
	}

	public String getRepost_from_site() {
		return repost_from_site;
	}

	public void setRepost_from_site(String repost_from_site) {
		this.repost_from_site = repost_from_site;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getRepost_from_url() {
		return repost_from_url;
	}

	public void setRepost_from_url(String repost_from_url) {
		this.repost_from_url = repost_from_url;
	}

	public String getSection() {
		return section;
	}

	public void setSection(String section) {
		this.section = section;
	}

	public String getCrawler_time() {
		return crawler_time;
	}

	public void setCrawler_time(String crawler_time) {
		this.crawler_time = crawler_time;
	}


	public String getVerified_type() {
		return verified_type;
	}

	public void setVerified_type(String verified_type) {
		this.verified_type = verified_type;
	}

	public String getDownload_type() {
		return download_type;
	}

	public void setDownload_type(String download_type) {
		this.download_type = download_type;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getTitle_top() {
		return title_top;
	}

	public void setTitle_top(String title_top) {
		this.title_top = title_top;
	}

	public String getCreated_date() {
		return created_date;
	}

	public void setCreated_date(String created_date) {
		this.created_date = created_date;
	}

	public String getPic() {
		return pic;
	}

	public void setPic(String pic) {
		this.pic = pic;
	}


	public String getSourceMid() {
		return sourceMid;
	}
	public void setSourceMid(String sourceMid) {
		this.sourceMid = sourceMid;
	}
    
    public String getProfileImageUrl() {
		return profileImageUrl;
	}

	public void setProfileImageUrl(String profileImageUrl) {
		this.profileImageUrl = profileImageUrl;
	}

	

	public String getUpdatetime() {
		return new Timestamp(System.currentTimeMillis()).toString();
	}

	public void setUpdatetime(String updatetime) {
		this.updatetime = updatetime;
	}

	


	public String getText_loc() {
		return text_loc;
	}

	public void setText_loc(String text_loc) {
		this.text_loc = text_loc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getWeibo_url() {
		return weibo_url;
	}

	public void setWeibo_url(String weibo_url) {
		this.weibo_url = weibo_url;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getMid_f() {
		return mid_f;
	}

	public void setMid_f(String mid_f) {
		this.mid_f = mid_f;
	}

	public String getMid_p() {
		return mid_p;
	}

	public void setMid_p(String mid_p) {
		this.mid_p = mid_p;
	}


	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Timestamp getCreated_at() {
		return created_at;
	}

	public void setCreated_at(Timestamp created_at) {
		this.created_at = created_at;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getText_loc_country() {
		return text_loc_country;
	}

	public void setText_loc_country(String text_loc_country) {
		this.text_loc_country = text_loc_country;
	}

	public String getText_loc_province() {
		return text_loc_province;
	}

	public void setText_loc_province(String text_loc_province) {
		this.text_loc_province = text_loc_province;
	}

	public String getText_loc_city() {
		return text_loc_city;
	}

	public void setText_loc_city(String text_loc_city) {
		this.text_loc_city = text_loc_city;
	}

	public String getEmotion() {
		return emotion;
	}

	public void setEmotion(String emotion) {
		this.emotion = emotion;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUser_local() {
		return user_local;
	}

	public void setUser_local(String user_local) {
		this.user_local = user_local;
	}

	public String getText_subject() {
		return text_subject;
	}

	public void setText_subject(String text_subject) {
		this.text_subject = text_subject;
	}



	

	public String getPic_local() {
		return pic_local;
	}

	public void setPic_local(String pic_local) {
		this.pic_local = pic_local;
	}

	public String getMedia_local() {
		return media_local;
	}

	public void setMedia_local(String media_local) {
		this.media_local = media_local;
	}

	// public long getScore() {
	// return score;
	// }
	//
	// public void setScore(long score) {
	// this.score = score;
	// }



	public String getIndustries_tag() {
		return industries_tag;
	}

	public void setIndustries_tag(String industries_tag) {
		this.industries_tag = industries_tag;
	}

	/**
	 * Returns a string representation of the object. In general, the
	 * {@code toString} method returns a string that "textually represents" this
	 * object. The result should be a concise but informative representation
	 * that is easy for a person to read. It is recommended that all subclasses
	 * override this method.
	 * <p>
	 * The {@code toString} method for class {@code Object} returns a string
	 * consisting of the name of the class of which the object is an instance,
	 * the at-sign character `{@code @}', and the unsigned hexadecimal
	 * representation of the hash code of the object. In other words, this
	 * method returns a string equal to the value of: <blockquote>
	 * 
	 * <pre>
	 * getClass().getName() + '@' + Integer.toHexString(hashCode())
	 * </pre>
	 * 
	 * </blockquote>
	 *
	 * @return 返回改实体bean名称以及标示字段.
	 */
/*	@Override
	public String toString() {
		return "Article{" +
				"key='" + key + '\'' +
				", mid='" + mid + '\'' +
				", events_tag='" + events_tag + '\'' +
				", source='" + source + '\'' +
				", created_at=" + created_at +
				", uid='" + uid + '\'' +
				", text='" + text + '\'' +
				", reposts_count=" + reposts_count +
				", comments_count=" + comments_count +
				", read_count=" + read_count +
				", zan_count=" + zan_count +
				", name='" + name + '\'' +
				", weibo_url='" + weibo_url + '\'' +
				", tags=" + tags +
				", updatetime='" + updatetime + '\'' +
				", sourceMid='" + sourceMid + '\'' +
				", download_type='" + download_type + '\'' +
				", crawler_site_id='" + crawler_site_id + '\'' +
				", url='" + url + '\'' +
				", repost_from_url='" + repost_from_url + '\'' +
				", crawler_time='" + crawler_time + '\'' +
				", article_type=" + article_type +
				", title='" + title + '\'' +
				", me=" + me +
				", force='" + force + '\'' +
				", categoryId=" + categoryId +
				'}';
	}*/
		@Override
	public String toString() {
		return "{'obj':{'index':'i_ams_total_data','type':'t_article','id':'mid'}}";
	}
}
