package com.cloudera;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RBucketAsync;
import org.redisson.api.RFuture;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author Charles
 * @package PACKAGE_NAME
 * @classname com.cloudera.RedissonAPiTest
 * @description TODO
 * @date 2019-10-17 13:01
 */
public class RedissonAPITest extends BaseTest {


    /**
     *
     */
    @Test
    public void testRBucket() {

        RBucket<Object> rBucket = redisson.getBucket("test");
        rBucket.setAsync("123", 240, TimeUnit.SECONDS);
        Object o = rBucket.get();
        System.out.println(o);

    }

    /**
     *
     */
    @Test
    public void testRBucketAsync() {
        RBucketAsync<String> rBuket = redisson.getBucket("testtest");
        rBuket.setAsync(new Random().nextInt() + "", 3600, TimeUnit.SECONDS);
        String s = ((RBucket<String>) rBuket).get();
        System.out.println(s);
    }

    /**
     * 删除单条
     */
    @Test
    public void testDeleteAsync() {
        RBucketAsync<String> batchBuket = redisson.getBucket("test:test-");
//        batchBuket.setAsync(i + "", 3600, TimeUnit.SECONDS);
        RFuture<Boolean> bool = batchBuket.deleteAsync();
        System.out.println(bool);
    }

    /**
     * 批量删除
     */
    @Test
    public void testDeleteByPattern() {
        String partten = "F*";
        Iterable<String> keysByPattern = redisson.getKeys().getKeysByPattern("test*");
        for (String key1 : keysByPattern) {
            System.out.println(key1);
        }
        Iterator<String> iterator = keysByPattern.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        redisson.getBucket("test1").set("someValue");
        redisson.getBucket("test2").set("someValue");

        redisson.getBucket("test12").set("someValue");
        redisson.getBucket("testtest").set("someValue");

        Iterator<String> iterator1 = redisson.getKeys().getKeysByPattern("test*").iterator();
        for (; iterator1.hasNext();) {
            String key = iterator1.next();
//            assertThat(key).isIn("test1", "test2");
            System.out.println(key);
        }
    }

}
