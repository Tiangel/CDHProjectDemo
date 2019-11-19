package com.cloudera.flume;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatUtil {

    public static Date timeToDate(double time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        int length = String.valueOf((long) time).length();
        if(length == 10 ){
            time = time * 1000;
        }
        String format = sdf.format(time);
        Date date = sdf.parse(format);
        return date;
    }

    public static Date strToDate (String dateStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = null;
        try {
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }
}
