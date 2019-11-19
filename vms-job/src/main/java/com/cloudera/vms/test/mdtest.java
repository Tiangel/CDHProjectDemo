package com.cloudera.vms.test;

import static com.cloudera.vms.jobs.ArticleDistributionJob.MD5;

/**
 * Created by Twin on 2018/8/23.
 */
public class mdtest {
    public static void main(String[] args) {
        String s = "afeo0DNmhl/OJ+l3Ykst8g==";

        String ss= MD5(s) + s;
        String sss=MD5(ss)+ss;
        System.out.println(sss);

    }
}
