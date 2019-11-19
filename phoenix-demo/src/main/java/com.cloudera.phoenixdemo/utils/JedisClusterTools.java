package com.cloudera.phoenixdemo.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Component
public class JedisClusterTools {

    @Value("${spring.redis.cluster.nodes}")
    private String redisClusterNodes;
    @Value("${spring.redis.password}")
    private String redisClusterPass;

    private JedisCluster jedisCluster;

    @Bean
    public JedisCluster JedisClusterTool () {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxWaitMillis(1500);
        Set<HostAndPort> nodes= new LinkedHashSet<HostAndPort>();
        System.out.println(redisClusterNodes);
        String[] split = redisClusterNodes.split(",");
        for (int i=0;i<split.length;i++) {
            nodes.add(new HostAndPort(redisClusterNodes.split(",")[i].split(":")[0],Integer.valueOf(redisClusterNodes.split(",")[i].split(":")[1])));
        }
        if (this.jedisCluster == null) {
            this.jedisCluster = new JedisCluster(nodes,2000,2000,20,redisClusterPass,poolConfig);
        }

        return  this.jedisCluster;
    }

    public Set<String> getKeys(String patten) {
        TreeSet<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = this.JedisClusterTool().getClusterNodes();
        for (String key : clusterNodes.keySet()) {
                JedisPool jedisPool = clusterNodes.get(key);
                Jedis jedisConn = jedisPool.getResource();
            try {
                keys.addAll(jedisConn.keys(patten));
            } finally {
                jedisConn.close();
            }

        }
        return keys;
    }

}
