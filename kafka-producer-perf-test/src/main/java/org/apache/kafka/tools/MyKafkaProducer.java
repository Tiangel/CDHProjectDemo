package org.apache.kafka.tools;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class MyKafkaProducer {

    private static AtomicLong totalSendSucceed = new AtomicLong(0);
    private static AtomicLong totalSendFail = new AtomicLong(0);
    private static long begin = System.currentTimeMillis();
    private static String topic = "passenger_flow_count_to_datacenter";
    private static String STRING_RANDOM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    // private static long       VALVE            = 1;//30 * 10000 + 367;

    //    private static String[] levels = new String[]{"DEBUG", "INFO", "WARN", "ERROR"};
    private static String[] deviceIds = new String[]{"A", "B"};

    public static byte[] getData(String dataConfig, String datatype) {
        //构造send对象
        Random random = new Random();

        List<String> list = getDataConfig(dataConfig);
        String[] arr = null;
        for (int i = 1; i < list.size(); i++) {
            arr = list.get(i).split("\t");
            if (arr.length == 3) {

            }
        }


        String[] typeArr = list.get(0).split("\t");
        if ("json".equalsIgnoreCase(typeArr[1])) {
//            generateJsonData(list);
        } else if ("string".equalsIgnoreCase(typeArr[1])) {

        }


        String value = "";
//                String.format("307100044B8FA5D%s00%s|PassengerRecord|307100044B8FA5DA0001|%s|60|%s|%s", deviceIds[random.nextInt(2)], String.format("%02d", random.nextInt(20)), new Timestamp(random.nextInt(10000000) + System.currentTimeMillis()), random.nextInt(50), random.nextInt(50));
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String generateStringData(List<String> list) {
        String[] arr = null;

        String[] typeArr = list.get(0).split("\t");
        if ("json".equalsIgnoreCase(typeArr[1])) {
//            generateJsonData(list);
        } else if ("string".equalsIgnoreCase(typeArr[1])) {
            String separator = typeArr[2];
            StringBuffer sb = new StringBuffer();
            for (int i = 1; i < list.size(); i++) {
                arr = list.get(i).split("\t");
                if (arr.length == 3) {
                    if (DataType.STRING.toString().equalsIgnoreCase(arr[1])) {
                        sb.append(generateRandomString(Integer.valueOf(arr[2])));
                    }else if(DataType.INT.toString().equalsIgnoreCase(arr[1])){
                        sb.append(generateRandomInt(Integer.valueOf(arr[2])));
                    }else if(DataType.DOUBLE.toString().equalsIgnoreCase(arr[1])){
                        sb.append(generateRandomString(Integer.valueOf(arr[2])));
                    }else if(DataType.DATE.toString().equalsIgnoreCase(arr[1])){
                        sb.append(generateRandomDate(Integer.valueOf(arr[2])));
                    }else if(DataType.TIMESTAMP.toString().equalsIgnoreCase(arr[1])){
                        sb.append(generateRandomString(Integer.valueOf(arr[2])));
                    }
                    if(i != list.size()-1){
                        sb.append(separator);
                    }
                }
            }
        }


        return "";
    }

    private static String generateRandomString(int len) {
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < len; i++) {
            int number = random.nextInt(62);
            sb.append(STRING_RANDOM.charAt(number));
        }
        return sb.toString();
    }
    private static String generateRandomInt(Integer max) {
        Random random = new Random();
        long rangeLong = random.nextInt(max);
        return rangeLong+"";
    }
    private static String generateRandomDouble(Integer max) {
        Random random = new Random();
        double d = random.nextDouble();
        return d+"";
    }
    private static String generateRandomDate(Integer max) {
        Random random = new Random();
        double d = random.nextDouble();
        return d+"";
    }


    private static List<String> getDataConfig(String path) {
        ArrayList<String> list = new ArrayList<>();
        BufferedReader reader = null;
        StringBuffer sbf = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(path));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                list.add(tempStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return list;
    }

    public static void main(String[] args) {
    }


//    private static void sendSingle(KafkaProducer producer, int index) {
//        long recordTime = System.currentTimeMillis();
//        //构造send对象
//        Random random = new Random();
//        String value = String.format("307100044B8FA5D%s00%s|PassengerRecord|307100044B8FA5DA0001|%s|60|%s|%s",
//                deviceIds[random.nextInt(2)], String.format("%02d", random.nextInt(20)),
//                new Timestamp(random.nextInt(10000000) + System.currentTimeMillis()), random.nextInt(50), random.nextInt(50));
//
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
//        try {
//            Future future = producer.send(record,
//                    new DemoCallback(recordTime, topic, "value_" + recordTime));
//            //future.get();
//            totalSendSucceed.incrementAndGet();
//            // System.out.println("本条发送完毕");
//        } catch (Exception e) {
//            totalSendFail.incrementAndGet();
//            System.out.println(e.toString());
//        } finally {
//            //System.exit(-1);
//        }
//    }
//
//    private static void unitTest() {
//        KafkaProducer<String, String> producer = getKafkaProducer();
//        //
//        int total = 100;
//        int mod = 1;
//        for (int index = 0; index < total; index++) {
//            Send send = new Send();
//            send.setPro("pro" + Math.abs(new Random().nextInt()) % mod);
//            send.setThrowable("throwable" + Math.abs(new Random().nextInt()) % mod);
//            send.setLevel("level" + Math.abs(new Random().nextInt()) % mod);
//            send.setIp("ip" + Math.abs(new Random().nextInt()) % mod);
//            long recordTime = System.currentTimeMillis();
//            String value = JSON.toJSONString(send);
//            //System.out.println(value);
//            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,
//                    value);
//            try {
//                Future future = producer.send(record,
//                        new DemoCallback(recordTime, topic, "value_" + recordTime));
//                //future.get();
//                totalSendSucceed.incrementAndGet();
//                // System.out.println("本条发送完毕");
//            } catch (Exception e) {
//                totalSendFail.incrementAndGet();
//                System.out.println(e.toString());
//            } finally {
//                System.out.println("生产完毕");
//            }
//        }
//
//    }
//
//    public static void main(String[] args) {
//
//        //                unitTest();
//        //                try {
//        //                    Thread.sleep(1000);
//        //                } catch (InterruptedException e) {
//        //                    //logger.error("", e);
//        //                }
//        //                unitTest();
//        //                try {
//        //                    Thread.sleep(1000);
//        //                } catch (InterruptedException e) {
//        //                    //logger.error("", e);
//        //                }
//        //                unitTest();
//
//        int tag = 30;
//        Thread thread = null;
//        //负责发送1
//        if (tag >= 1) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 1, 90000001L);//
//                }
//            });
//            thread.start();
//        }
//
//        //负责发送2
//        if (tag >= 2) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 2, 90000002L);//
//                }
//            });
//            thread.start();
//        }
//        //负责发送3
//        if (tag >= 3) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 3, 90000003L);//
//                }
//            });
//            thread.start();
//        }
//        //负责发送4
//        if (tag >= 4) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 4, 90000004L);//
//                }
//            });
//            thread.start();
//        }
//        //负责发送5
//        if (tag >= 5) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 5, 90000005L);//
//                }
//            });
//            thread.start();
//        }
//        //负责发送6
//        if (tag >= 6) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 6, 90000006L);//
//                }
//            });
//            thread.start();
//        }
//        //负责发送7
//        if (tag >= 7) {
//            thread = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    main0(args, 7, 90000007L);//
//                }
//            });
//            thread.start();
//        }
//        //统计线程
//        Thread statsThread = new Thread(new Runnable() {
//
//            @Override
//            public void run() {
//                long last = System.currentTimeMillis();
//                long lastsuc = 0;
//                long lastfail = 0;
//                //
//                while (true) {
//                    long now = System.currentTimeMillis();
//                    long elapsed = now - last;
//                    if (elapsed >= 1000) {
//                        long currentSuc = totalSendSucceed.get();
//                        long currentFail = totalSendFail.get();
//                        System.out
//                                .println("elapsed " + elapsed + " - suc " + (currentSuc - lastsuc)
//                                        + " total " + currentSuc + " fail " + (currentFail - lastfail)
//                                        + " " + System.currentTimeMillis());
//                        //
//                        last = now;
//                        lastsuc = currentSuc;
//                        lastfail = currentFail;
//                    }
//                }
//                //end
//            }
//        });
//        statsThread.start();
//        //
//    }
//
//    private static KafkaProducer<String, String> getKafkaProducer() {
//        Properties kafkaProps = new Properties();
//
//        kafkaProps.put("bootstrap.servers", "10.101.72.32:9092,10.101.72.33:9092,10.101.72.34:9092");
//        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        //网络调优
//        //1)发送
//        kafkaProps.put("compression.type", "lz4");
//        kafkaProps.put("batch.size", "8196");
//        //2)
//        //kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1024000");
//        // kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1000");
//        //2)等待响应
//        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
//        kafkaProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");
//        //3)超时重试
//        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, 30);
//        //
//        // kafkaProps.put("batch.size", "10240");
//        // kafkaProps.put("linger.ms", "0");
//
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
//        return producer;
//    }
//
//    public static void main0(String[] args, int index, Long valve) {
//        //
//        KafkaProducer<String, String> producer = getKafkaProducer();
//        System.out.println("发送开始 ");
//        long sendCount = 0;
//        while (true) {
//            try {
//                sendSingle(producer, index);
//            } catch (Exception e) {
//                System.out.println(e.toString());
//            } finally {
//                sendCount++;
//                if (sendCount >= valve) {
//                    //线程结束
//                    System.out.println("send " + index + " for " + valve + " 条 完毕!");
//                    producer.close();
//                    return;
//                }
//            }
//
//        }
//
//    }
//
//    static class DemoCallback implements Callback {
//        private long startTime;
//        @SuppressWarnings("unused")
//        private String key;
//        @SuppressWarnings("unused")
//        private String message;
//
//        public DemoCallback(long startTime, String key, String message) {
//            this.startTime = startTime;
//            this.key = key;
//            this.message = message;
//        }
//
//        @Override
//        public void onCompletion(RecordMetadata metadata, Exception exception) {
//            long elapsedTime = System.currentTimeMillis() - startTime;
//            if (null != exception) {
//                System.out.println(exception.toString());
//                return;
//            }
//            //            if (null != metadata && elapsedTime >= 1000) {
//            //                 System.out.println("message(" + key + ", " + message + ") sent to partition("
//            //                 + metadata.partition()
//            //                 + "), " + "offset(" + metadata.offset() + " ) in " + elapsedTime + " ms");
//            //
//            //            }
//
//        }
//
//    }

}
