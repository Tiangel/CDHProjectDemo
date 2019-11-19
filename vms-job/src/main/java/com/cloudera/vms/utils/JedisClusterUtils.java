package com.cloudera.vms.utils;

import com.cloudera.vms.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;


public class JedisClusterUtils {

    static JedisCluster jedisCluster = null;
    static JedisPool jedisPool = null;
    static Jedis jedis = null;
    //static int expireTime = 60 * 60 * 24 * 7;//7天

    public static boolean isExists(String key, String value,int expireTime) {
        //华为云单点
        //Jedis jedis = getJedis();
        try {
            //华为云集群
            JedisCluster jedis = getJedisCluster();//todo 就集群模式
            if (!jedis.exists(key)) {
                jedis.setex(key, expireTime, value);//value is "";
                return false;
            } else {
                jedis.expire(key, expireTime);//设置键key的过期时间
                return true;
            }
        } catch (Exception e) {
            System.out.println("  redis can't work well ");
            e.printStackTrace();
            return false;
        } finally {
            if(jedis !=null){
                //jedis.close();
            }
        }
    }
    public static JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            synchronized (JedisClusterUtils.class) {
                if (jedisCluster == null) {
                    JedisClusterManager jcManager = new JedisClusterManager();
                    jedisCluster = jcManager.getRedisCluster();
                }
            }

        }
        return jedisCluster;
    }

    public static Jedis getJedis() {
        if (jedisPool == null) {
            synchronized (JedisClusterUtils.class) {
                if (jedisPool == null) {
                    JedisClusterManager jcManager = new JedisClusterManager();
                    jedisPool = jcManager.getJedisPool();
                }
            }
        }
        Jedis jedis = jedisPool.getResource();
        String pwd = Config.get("redis.pwd");
        if( StringUtils.isNotBlank(pwd))
        	jedis.auth(pwd);
//        String password = "Izh@23DysQ";//华为云密码
//        jedis.auth(password);
        return jedis;
    }


    public static void main(String[] args) {
       JedisCluster jedis = JedisClusterUtils.getJedisCluster();
       ScanResult<Entry<String,String>> result = jedis.hscan("sim-group-meta", "0");
       
       System.out.println(result.getStringCursor());
       
    }
}

class JedisClusterManager {
    public JedisCluster getRedisCluster() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        String redisURLs = Config.get(Config.KEY_REDIS_URL);
        String[] redisURLArray = redisURLs.split("\\,");
        for (String redisURL : redisURLArray) {
            String ip = redisURL.split(":")[0];
            String port = redisURL.split(":")[1];
            jedisClusterNodes.add(new HostAndPort(ip, Integer.parseInt(port)));
        }
//		jedisClusterNodes.add(new HostAndPort("10.248.161.7", 7000));
//		jedisClusterNodes.add(new HostAndPort("10.248.161.7", 7001));
//		jedisClusterNodes.add(new HostAndPort("10.248.161.7", 7002));

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(20);
        return new JedisCluster(jedisClusterNodes, 1000, 10,config);
    }


    public JedisPool getJedisPool() {

        String redisURLs = Config.get(Config.KEY_REDIS_URL);
        String[] redisURLArray = redisURLs.split("\\,");
        String ip = redisURLArray[0].split(":")[0];
        String port = redisURLArray[0].split(":")[1];

        JedisPoolConfig config = new JedisPoolConfig();
        //设置最大连接数, 默认8个
//        config.setMaxTotal(50);
        config.setMaxTotal(-1);////如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
//        config.setMaxTotal(-1);
        //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        config.setMaxWaitMillis(2000);
        //设置最大空闲连接数, 默认8个
        config.setMaxIdle(20);
        //初始化jedisPool,设置IP和端口号
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), ip, Integer.valueOf(port));


        return jedisPool;
    }
}