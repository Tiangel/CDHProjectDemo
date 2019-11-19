package com.cloudera;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class BaseTest {

    protected RedissonClient redisson;
    protected static RedissonClient defaultRedisson;

    protected static Properties properties;

    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException {
        InputStream in = null;
        try {
            //文件要放到resource文件夹下
            in = BaseTest.class.getClassLoader().getResourceAsStream("redisson.properties");
            Properties prop = new Properties();
            prop.load(in);
            properties = prop;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("配置文件加载失败！");
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        defaultRedisson = createInstance();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                defaultRedisson.shutdown();
            }
        });
    }

    @Before
    public void before() throws IOException, InterruptedException {
        if (redisson == null) {
            redisson = defaultRedisson;
        }
        if (flushBetweenTests()) {
            redisson.getKeys().flushall();
        }
    }

    @After
    public void after() throws InterruptedException {
        redisson.shutdown();
    }

    public static Config createConfig() {
        String[] redisHost = properties.getProperty("redis.redisson.host").split(",");
        String redisPassword = properties.getProperty("redis.password");
        Config config = new Config();
        config.setCodec(new StringCodec()).useClusterServers()
                // 集群状态扫描间隔时间，单位是毫秒
                .setScanInterval(2000).addNodeAddress(redisHost).setPassword(redisPassword);
        return config;
    }

    public static RedissonClient createInstance() {
        Config config = createConfig();
        return Redisson.create(config);
    }

    protected boolean flushBetweenTests() {
        return true;
    }

}