//package com.cloudera;
//
//import java.time.*;
//import java.time.format.DateTimeFormatter;
//import java.time.temporal.ChronoField;
//import java.time.temporal.ChronoUnit;
//
///**
// * @author Charles
// * @package com.cloudera
// * @classname LocalDateUtils
// * @description TODO
// * @date 2019-12-4 9:10
// */
//public class LocalDateUtils {
//
//
//    public void testLocalDate(){
//        //获取当前年月日
//        LocalDate localDate = LocalDate.now();
//        //构造指定的年月日
//        LocalDate localDate1 = LocalDate.of(2019, 9, 10);
//        System.out.println(localDate);
//        System.out.println(localDate1);
//        int year = localDate.getYear();
//        int year1 = localDate.get(ChronoField.YEAR);
//        Month month = localDate.getMonth();
//        int month1 = localDate.get(ChronoField.MONTH_OF_YEAR);
//        int day = localDate.getDayOfMonth();
//        int day1 = localDate.get(ChronoField.DAY_OF_MONTH);
//        DayOfWeek dayOfWeek = localDate.getDayOfWeek();
//        int dayOfWeek1 = localDate.get(ChronoField.DAY_OF_WEEK);
//    }
//
//    public void testLocalTime(){
//        LocalTime localTime = LocalTime.of(13, 51, 10);
//        LocalTime localTime1 = LocalTime.now();
//        //获取小时
//        int hour = localTime.getHour();
//        int hour1 = localTime.get(ChronoField.HOUR_OF_DAY);
//        // 获取分
//        int minute = localTime.getMinute();
//        int minute1 = localTime.get(ChronoField.MINUTE_OF_HOUR);
//        //获取秒
//        int second = localTime.getSecond();
//        int second1 = localTime.get(ChronoField.SECOND_OF_MINUTE);
//    }
//
//    public void testLocalDateTime(){
//        LocalDateTime localDateTime = LocalDateTime.now();
//        LocalDateTime localDateTime1 = LocalDateTime.of(2019, Month.SEPTEMBER, 10, 14, 46, 56);
//
//
//        LocalDate localDate2 = localDateTime.toLocalDate();
//        LocalTime localTime2 = localDateTime.toLocalTime();
//
//        //        LocalDateTime localDateTime2 = LocalDateTime.of(localDate, localTime);
////        LocalDateTime localDateTime3 = localDate.atTime(localTime);
////        LocalDateTime localDateTime4 = localTime.atDate(localDate);
//    }
//
//
//    public static void main(String[] args) {
//
//
//
//
//
//
//        Instant instant = Instant.now();
//        long currentSecond = instant.getEpochSecond();
//        long currentMilli = instant.toEpochMilli();
//        LocalDateTime localDateTime = LocalDateTime.of(2019, Month.SEPTEMBER, 10, 14, 46, 56);
//        //增加一年
//        localDateTime = localDateTime.plusYears(1);
//        localDateTime = localDateTime.plus(1, ChronoUnit.YEARS);
//        //减少一个月
//        localDateTime = localDateTime.minusMonths(1);
//        localDateTime = localDateTime.minus(1, ChronoUnit.MONTHS);
//        //修改年为2019
//        localDateTime = localDateTime.withYear(2020);
//        //修改为2022
//        localDateTime = localDateTime.with(ChronoField.YEAR, 2022);
//        LocalDate localDate = LocalDate.now();
//        LocalDate localDate1 = localDate.with(firstDayOfYear());
//        LocalDate localDate = LocalDate.of(2019, 9, 10);
//        String s1 = localDate.format(DateTimeFormatter.BASIC_ISO_DATE);
//        String s2 = localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
//        //自定义格式化
//        DateTimeFormatter dateTimeFormatter =   DateTimeFormatter.ofPattern("dd/MM/yyyy");
//        String s3 = localDate.format(dateTimeFormatter);
//        LocalDate localDate1 = LocalDate.parse("20190910", DateTimeFormatter.BASIC_ISO_DATE);
//        LocalDate localDate2 = LocalDate.parse("2019-09-10", DateTimeFormatter.ISO_LOCAL_DATE)
//    }
//}
