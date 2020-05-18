package com.cloudera.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.flume.utils.DateFormatUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LogInterceptor implements Interceptor {

    private boolean preserveExisting;
    private String logType;

    private LogInterceptor() {

    }
    private LogInterceptor(boolean preserveExisting,String logType) {
        this.preserveExisting = preserveExisting;
        this.logType = logType;
    }

    public void initialize() {
    }

    public Event intercept(Event event) {
		byte[] body = event.getBody();
        String bodyStr = new String(body);
        String[] s = bodyStr.split("\\|");
        Date date = DateFormatUtil.strToDate(s[0].substring(1));
        String logType= s[1];
        String beanStr = "";
//        if(appType.equals(type)){
            JSONObject jsonObject = JSON.parseObject(s[2]);
            String referrer = jsonObject.get("referrer")==null?"":(String) jsonObject.get("referrer");
            String mark = jsonObject.get("mark")==null?"":(String) jsonObject.get("mark");
            boolean flag = referrer.matches("^https?:.+:\\d.+") || mark.matches("^https?:.+:\\d.+");
            if(!flag){
                jsonObject.put("serverTime", date);
                beanStr = jsonObject.toJSONString();
            }
//        }
        event.setBody(beanStr.getBytes(Charset.forName("UTF-8")));
             
        Map<String, String> headers = event.getHeaders();
        headers.put(LogInterceptor.Constants.LOGTYPE, logType);
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        Iterator i$ = events.iterator();
        while (i$.hasNext()) {
            Event event = (Event) i$.next();
            this.intercept(event);
        }
        return events;
    }

    public void close() {
    }

    public static class Constants {
        public static String TIMESTAMP = "timestamp";
        public static String PRESERVE = "preserveExisting";
        public static String LOGTYPE = "type";
        public static boolean PRESERVE_DFLT = false;
        public Constants() {
        }
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        private boolean preserveExisting;
        private String logType;

        public Builder() {
            this.preserveExisting = LogInterceptor.Constants.PRESERVE_DFLT;
        }

        public Interceptor build() {

            return new LogInterceptor(this.preserveExisting,this.logType);
        }

        public void configure(Context context) {
            this.preserveExisting = context.getBoolean(LogInterceptor.Constants.PRESERVE, LogInterceptor.Constants.PRESERVE_DFLT);
            this.logType = context.getString(Constants.LOGTYPE);
        }
    }
}