package org.apache.kafka.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
 
public class MyQueue {
    // poll: 若队列为空，返回null。
    // remove:若队列为空，抛出NoSuchElementException异常。
    // take:若队列为空，发生阻塞，等待有元素。

    // put---无空间会等待
    // add--- 满时,立即返回,会抛出异常
    // offer---满时,立即返回,不抛异常
    // private static final Logger logger =
    // LoggerFactory.getLogger(MonitorQueue.class);
    public static BlockingQueue<byte[]> objectQueue = new LinkedBlockingQueue<byte[]>(50 * 10000);

    public static void addObject(byte[] obj) {
        objectQueue.offer(obj);
    }

    public static byte[] getObject() throws InterruptedException {
        //System.out.println(objectQueue.size());
        return objectQueue.take();
    }

    private static List list = new ArrayList();

    static {
        //启动时,开启线程
        int total = 3;
        for (int index = 0; index < total; index++) {
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (true) {
                        byte[] data = MyKafkaProducer.getData();
                        addObject(data);
                    }
                }

            });
            thread.start();
            list.add(thread);
        }
    }
}
