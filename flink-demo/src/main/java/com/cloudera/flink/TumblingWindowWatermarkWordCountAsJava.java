package com.cloudera.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import javax.annotation.Nullable;

public class TumblingWindowWatermarkWordCountAsJava {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //这里为了便于理解，设置并行度为1,默认并行度是当前机器的cpu数量
        senv.setParallelism(1);
        /*指定数据源 从socket的9000端口接收数据，先进行了不合法数据的过滤*/
        DataStream<String> sourceDS = senv.socketTextStream("bigdata-dev-kafka-01", 7777)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        if (null == line || "".equals(line)) {
                            return false;
                        }
                        String[] lines = line.split(",");
                        if (lines.length != 2) {
                            return false;
                        }
                        return true;
                    }
                });
        /*做了一个简单的map转换，将数据转换成Tuple2<long,String,Integer>格式，第一个字段代表是时间 第二个字段代表的是单词,第三个字段固定值出现了1次*/
        DataStream<Tuple3<Long, String, Integer>> wordDS = sourceDS.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] lines = line.split(",");

                return new Tuple3<Long, String, Integer>(Long.valueOf(lines[0]), lines[1], 1);
            }
        });
        /*设置Watermark的生成方式为Periodic Watermark，并实现他的两个函数getCurrentWatermark和extractTimestamp*/

        DataStream<Tuple3<Long, String, Integer>> wordCount = wordDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, Integer>>() {
            private Long currentMaxTimestamp = 0L;
            /*最大允许的消息延迟是5秒*/
            private final Long maxOutOfOrderness = 5000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element, long previousElementTimestamp) {
                long timestamp = element.f0;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
            /*这里根据第二个元素  单词进行统计 时间窗口是30秒  最大延时是5秒，统计每个窗口单词出现的次数*/
        }).keyBy(1)
                /*时间窗口是30秒*/
                .timeWindow(Time.seconds(30))
                .sum(2);

        wordCount.print("\n单词统计：");
        senv.execute("Window WordCount");
    }
}
