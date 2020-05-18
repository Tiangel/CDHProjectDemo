package com.cloudera.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(JsonInterceptor.class);
    private boolean preserveExisting;
    private String sourceKeys;
    private String targetKeys;

    private JsonInterceptor() {

    }

    private JsonInterceptor(boolean preserveExisting, String sourceKeys, String targetKeys) {
        this.preserveExisting = preserveExisting;
        this.sourceKeys = sourceKeys;
        this.targetKeys = targetKeys;
    }

    public void initialize() {
    }

    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String bodyStr = new String(body);
        String[] sourceArray = sourceKeys.trim().split(",", -1);
        String[] targetArray = targetKeys.trim().toLowerCase().split(",", -1);
        Map resultMap = new HashMap<String, String>();
        if (sourceArray.length == targetArray.length) {
            JSONObject jsonObject = JSONObject.parseObject(bodyStr);
            JSONObject jsonObjectTemp = null;
            String[] arrayTemp = null;
            for (int i = 0; i < sourceArray.length; i++) {
                if (sourceArray[i].contains(".")) {
                    arrayTemp = sourceArray[i].trim().split("\\.", -1);
                    jsonObjectTemp = jsonObject;
                    for (int j = 0; j < arrayTemp.length - 1; j++) {
                        if (jsonObjectTemp != null) {
                            jsonObjectTemp = jsonObjectTemp.getJSONObject(arrayTemp[j].trim());
                        }else {
                            break;
                        }
                    }
                    if (jsonObjectTemp != null){
                        resultMap.put(targetArray[i].trim(), String.valueOf(jsonObjectTemp.getOrDefault(arrayTemp[arrayTemp.length - 1], "")));
                    }else {
                        resultMap.put(targetArray[i].trim(), "");
                    }
                } else {
                    resultMap.put(targetArray[i].trim(), String.valueOf(jsonObject.getOrDefault(sourceArray[i], "")));
                }
            }
        } else {
            logger.error("The sourceKeys and targetkeys lengths do not match");
        }
        event.setBody(JSON.toJSONString(resultMap).getBytes(Charset.forName("UTF-8")));
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
        public static String PRESERVE = "preserveExisting";
        public static String SOURCE_KEYS = "sourceKeys";
        public static String TARGET_KEYS = "targetKeys";
        public static boolean PRESERVE_DFLT = false;

        public Constants() {
        }
    }

    public static class Builder implements Interceptor.Builder {
        private boolean preserveExisting;
        private String sourceKeys;
        private String targetKeys;

        public Builder() {
            this.preserveExisting = JsonInterceptor.Constants.PRESERVE_DFLT;
        }

        public Interceptor build() {

            return new JsonInterceptor(this.preserveExisting, this.sourceKeys, this.targetKeys);
        }

        public void configure(Context context) {
            this.preserveExisting = context.getBoolean(JsonInterceptor.Constants.PRESERVE, JsonInterceptor.Constants.PRESERVE_DFLT);
            this.sourceKeys = context.getString(Constants.SOURCE_KEYS);
            this.targetKeys = context.getString(Constants.TARGET_KEYS);
        }
    }
}