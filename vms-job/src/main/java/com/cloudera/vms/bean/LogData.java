package com.cloudera.vms.bean;

/**
 * Created by Twin on 2018/9/11.
 */
public class LogData {
    private String uuid;//lnk_key+"_"+num，注：采集数据回传加入
    private String mid;//唯一id
    private String uid;//用户id
    private String url;//采集链接
    private String crawler_time; //采集时间
    private String state;//单条采集状态
    private String excetpion;//异常信息
    private String probably_because;//可能原因
    private Integer like_num;//点赞数
    private Integer read_num;//阅读数
    private Integer comment_num;//评论数
    private String created_at;//文章发布时间
    private Long process_time;//处理时间

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getCrawler_time() {
        return crawler_time;
    }

    public void setCrawler_time(String crawler_time) {
        this.crawler_time = crawler_time;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getExcetpion() {
        return excetpion;
    }

    public void setExcetpion(String excetpion) {
        this.excetpion = excetpion;
    }

    public String getProbably_because() {
        return probably_because;
    }

    public void setProbably_because(String probably_because) {
        this.probably_because = probably_because;
    }

    public Integer getLike_num() {
        return like_num;
    }

    public void setLike_num(Integer like_num) {
        this.like_num = like_num;
    }

    public Integer getRead_num() {
        return read_num;
    }

    public void setRead_num(Integer read_num) {
        this.read_num = read_num;
    }

    public Integer getComment_num() {
        return comment_num;
    }

    public void setComment_num(Integer comment_num) {
        this.comment_num = comment_num;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public Long getProcess_time() {
        return process_time;
    }

    public void setProcess_time(Long process_time) {
        this.process_time = process_time;
    }
}
