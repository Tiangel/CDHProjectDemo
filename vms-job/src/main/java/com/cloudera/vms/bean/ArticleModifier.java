package com.cloudera.vms.bean;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class ArticleModifier implements Cloneable, Serializable {

    private String mid; // 微博消息id，文档唯一标示.

    private Integer reports_count; // 转发数.

    private Integer comments_count; // 评论数.

    private Integer zans_count;

    private Integer read_count;

    private int article_type;

    private Timestamp created_at; // 文章发布时间
    private String updatetime;
    private String crawler_time;//采集时间
    private String history;//是否需要保存历史统计趋势
    private List<String> tags;//订阅功能的客户自己的组织id
    private String url;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getReports_count() {
        return reports_count;
    }

    public void setReports_count(Integer reports_count) {
        this.reports_count = reports_count;
    }

    public Integer getComments_count() {
        return comments_count;
    }

    public void setComments_count(Integer comments_count) {
        this.comments_count = comments_count;
    }

    public Integer getZans_count() {
        return zans_count;
    }

    public void setZans_count(Integer zans_count) {
        this.zans_count = zans_count;
    }

    public Integer getRead_count() {
        return read_count;
    }

    public void setRead_count(Integer read_count) {
        this.read_count = read_count;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Timestamp getCreated_at() {
        return created_at;
    }

    public void setCreated_at(Timestamp created_at) {
        this.created_at = created_at;
    }

    public String getCrawler_time() {
        return crawler_time;
    }

    public void setCrawler_time(String crawler_time) {
        this.crawler_time = crawler_time;
    }

    public String getHistory() {
        return history;
    }

    public void setHistory(String history) {
        this.history = history;
    }

    public int getArticle_type() {
        return article_type;
    }

    public void setArticle_type(int article_type) {
        this.article_type = article_type;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }


    public String getUpdatetime() {
        return new Timestamp(System.currentTimeMillis()).toString();
    }

    public void setUpdatetime(String updatetime) {
        this.updatetime = updatetime;
    }



}