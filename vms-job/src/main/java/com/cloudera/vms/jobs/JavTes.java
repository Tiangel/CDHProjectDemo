package com.cloudera.vms.jobs;

import com.cloudera.vms.utils.KafkaTest;
import com.izhonghong.vms.zookeeper.ZKUtils;

import java.util.Map;

/**
 * Created by Twin on 2018/11/9.
 */
public class JavTes {
    public static void main(String[] args) {

        String topic = args[0];
        String group="wt";
        Map<Integer, Long> earlistOffsets = null;
        try {
            Map<Integer, Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
            earlistOffsets = KafkaTest.getEarlistOffset(topic, 5);
            System.out.println(earlistOffsets);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
