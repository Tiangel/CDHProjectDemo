package com.cloudera.vms.utils;

import java.io.IOException;
import java.util.TimeZone;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by Twin on 2018/11/26.
 */
public class TimeHelper {
    public long time = System.currentTimeMillis();

    public TimeHelper() {
        try {
            time = init();
        } catch (IOException e) {
// TODO Auto-generated catch block
            e.printStackTrace();
        }
        new Thread(() -> {
            add();
        }).start();
    }

    public TimeHelper(long time) {
        this.time = time;
        new Thread(() -> {
            add();
        }).start();
    }

    public static void main(String[] args) {

        TimeHelper izhTimeHelper = new TimeHelper();
        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(izhTimeHelper.time);
        }

    }

    private static long init() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        URL url = new URL("http://www.baidu.com");
        URLConnection uc = url.openConnection();
        uc.connect();
        long ld = uc.getDate();
        return ld;
    }

    private void add() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            }
            time += 1000;
        }
    }
}
