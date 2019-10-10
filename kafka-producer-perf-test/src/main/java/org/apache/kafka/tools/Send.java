package org.apache.kafka.tools;

/**
 * @author Charles
 * @package org.apache.kafka.tools
 * @classname S
 * @description TODO
 * @date 2019-10-9 11:46
 */
public class Send {
    private  String msg;
    private  String loc;
    private  String stack;
    private  String timestamp;
    private  String level;
    private  String ip;
    private  String throwable;
    private  String timeInSec;
    private  String thread;
    private  String time;
    private  String type;
    private  String pro;

    public Send(){
    }

    public Send(String msg, String loc, String stack, String timestamp, String level, String ip, String throwable, String timeInSec, String thread, String time, String type, String pro) {
        this.msg = msg;
        this.loc = loc;
        this.stack = stack;
        this.timestamp = timestamp;
        this.level = level;
        this.ip = ip;
        this.throwable = throwable;
        this.timeInSec = timeInSec;
        this.thread = thread;
        this.time = time;
        this.type = type;
        this.pro = pro;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    public String getStack() {
        return stack;
    }

    public void setStack(String stack) {
        this.stack = stack;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getThrowable() {
        return throwable;
    }

    public void setThrowable(String throwable) {
        this.throwable = throwable;
    }

    public String getTimeInSec() {
        return timeInSec;
    }

    public void setTimeInSec(String timeInSec) {
        this.timeInSec = timeInSec;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPro() {
        return pro;
    }

    public void setPro(String pro) {
        this.pro = pro;
    }
}
