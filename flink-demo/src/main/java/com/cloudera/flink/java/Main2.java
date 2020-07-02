package com.cloudera.flink.java;


import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 *
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<WordEvent> data = env.addSource(new MyCustomSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>(){
                    private long currentTimestamp = Long.MIN_VALUE;

                    private final long maxTimeLag = 5000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
                        long timestamp = element.getTimestamp();
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }
                });
//        data.print();
        data.keyBy(WordEvent::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<WordEvent, WordEvent, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WordEvent> input, Collector<WordEvent> out) throws Exception {
                        System.out.println(window.getStart() + " " + window.getEnd() + " w");
                        for (WordEvent word : input) {
                            out.collect(word);
                        }
                    }
                })
//                .sum("count")
                .print();

        env.execute("zhisheng —— flink window example");
    }
}
