package org.apache.kafka.tools;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class MyKafkaProducer {

    private static AtomicLong totalSendSucceed = new AtomicLong(0);
    private static AtomicLong totalSendFail = new AtomicLong(0);
    private static long begin = System.currentTimeMillis();
    private static String topic = "passenger_flow_count_to_datacenter";
    // private static long       VALVE            = 1;//30 * 10000 + 367;

    //    private static String[] levels = new String[]{"DEBUG", "INFO", "WARN", "ERROR"};
    private static String[] deviceIds = new String[]{"A", "B"};

    public static byte[] getData() {
        //构造send对象
        Random random = new Random();

        String value = String.format("307100044B8FA5D%s00%s|PassengerRecord|307100044B8FA5DA0001|%s|60|%s|%s",
                deviceIds[random.nextInt(2)], String.format("%02d", random.nextInt(20)), new Timestamp(random.nextInt(10000000) + System.currentTimeMillis()), random.nextInt(50), random.nextInt(50));
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static void sendSingle(KafkaProducer producer, int index) {
        long recordTime = System.currentTimeMillis();
        //构造send对象

        Random random = new Random();
        String value = String.format("307100044B8FA5D%s00%s|PassengerRecord|307100044B8FA5DA0001|%s|60|%s|%s",
                deviceIds[random.nextInt(2)], String.format("%02d", random.nextInt(20)),
                new Timestamp(random.nextInt(10000000) + System.currentTimeMillis()), random.nextInt(50), random.nextInt(50));

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        try {
            Future future = producer.send(record,
                    new DemoCallback(recordTime, topic, "value_" + recordTime));
            //future.get();
            totalSendSucceed.incrementAndGet();
            // System.out.println("本条发送完毕");
        } catch (Exception e) {
            totalSendFail.incrementAndGet();
            System.out.println(e.toString());
        } finally {
            //System.exit(-1);
        }
    }

    private static void unitTest() {
        KafkaProducer<String, String> producer = getKafkaProducer();
        //
        int total = 100;
        int mod = 1;
        for (int index = 0; index < total; index++) {
            Send send = new Send();
            send.setPro("pro" + Math.abs(new Random().nextInt()) % mod);
            send.setThrowable("throwable" + Math.abs(new Random().nextInt()) % mod);
            send.setLevel("level" + Math.abs(new Random().nextInt()) % mod);
            send.setIp("ip" + Math.abs(new Random().nextInt()) % mod);
            long recordTime = System.currentTimeMillis();
            String value = JSON.toJSONString(send);
            //System.out.println(value);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,
                    value);
            try {
                Future future = producer.send(record,
                        new DemoCallback(recordTime, topic, "value_" + recordTime));
                //future.get();
                totalSendSucceed.incrementAndGet();
                // System.out.println("本条发送完毕");
            } catch (Exception e) {
                totalSendFail.incrementAndGet();
                System.out.println(e.toString());
            } finally {

            }
        }

    }

    public static void main(String[] args) {

        //                unitTest();
        //                try {
        //                    Thread.sleep(1000);
        //                } catch (InterruptedException e) {
        //                    //logger.error("", e);
        //                }
        //                unitTest();
        //                try {
        //                    Thread.sleep(1000);
        //                } catch (InterruptedException e) {
        //                    //logger.error("", e);
        //                }
        //                unitTest();

        int tag = 30;
        Thread thread = null;
        //负责发送1    
        if (tag >= 1) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 1, 90000001L);//
                }
            });
            thread.start();
        }

        //负责发送2
        if (tag >= 2) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 2, 90000002L);//
                }
            });
            thread.start();
        }
        //负责发送3
        if (tag >= 3) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 3, 90000003L);//
                }
            });
            thread.start();
        }
        //负责发送4
        if (tag >= 4) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 4, 90000004L);//
                }
            });
            thread.start();
        }
        //负责发送5
        if (tag >= 5) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 5, 90000005L);//
                }
            });
            thread.start();
        }
        //负责发送6
        if (tag >= 6) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 6, 90000006L);//
                }
            });
            thread.start();
        }
        //负责发送7
        if (tag >= 7) {
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    main0(args, 7, 90000007L);//
                }
            });
            thread.start();
        }
        //统计线程
        Thread statsThread = new Thread(new Runnable() {

            @Override
            public void run() {
                long last = System.currentTimeMillis();
                long lastsuc = 0;
                long lastfail = 0;
                //
                while (true) {
                    long now = System.currentTimeMillis();
                    long elapsed = now - last;
                    if (elapsed >= 1000) {
                        long currentSuc = totalSendSucceed.get();
                        long currentFail = totalSendFail.get();
                        System.out
                                .println("elapsed " + elapsed + " - suc " + (currentSuc - lastsuc)
                                        + " total " + currentSuc + " fail " + (currentFail - lastfail)
                                        + " " + System.currentTimeMillis());
                        //
                        last = now;
                        lastsuc = currentSuc;
                        lastfail = currentFail;
                    }
                }
                //end
            }
        });
        statsThread.start();
        //
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "10.101.72.32:9092,10.101.72.33:9092,10.101.72.34:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //网络调优
        //1)发送
        kafkaProps.put("compression.type", "lz4");
        kafkaProps.put("batch.size", "8196");
        //2)
        //kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1024000");
        // kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1000");
        //2)等待响应
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");
        //3)超时重试
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, 30);
        //
        // kafkaProps.put("batch.size", "10240");
        // kafkaProps.put("linger.ms", "0");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        return producer;
    }

    public static void main0(String[] args, int index, Long valve) {
        //
        KafkaProducer<String, String> producer = getKafkaProducer();
        System.out.println("发送开始 ");
        long sendCount = 0;
        while (true) {
            try {
                sendSingle(producer, index);
            } catch (Exception e) {
                System.out.println(e.toString());
            } finally {
                sendCount++;
                if (sendCount >= valve) {
                    //线程结束
                    System.out.println("send " + index + " for " + valve + " 条 完毕!");
                    producer.close();
                    return;
                }
            }

        }

    }

    static class DemoCallback implements Callback {
        private long startTime;
        @SuppressWarnings("unused")
        private String key;
        @SuppressWarnings("unused")
        private String message;

        public DemoCallback(long startTime, String key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (null != exception) {
                System.out.println(exception.toString());
                return;
            }
            //            if (null != metadata && elapsedTime >= 1000) {
            //                 System.out.println("message(" + key + ", " + message + ") sent to partition("
            //                 + metadata.partition()
            //                 + "), " + "offset(" + metadata.offset() + " ) in " + elapsedTime + " ms");
            //
            //            }

        }

    }

}
