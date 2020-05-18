//package com.cloudera.flink;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
//import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
//import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Objects;
//
//public class RedisSink<IN> extends RichSinkFunction<IN> {
//    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
//    private String additionalKey;
//    private RedisMapper<IN> redisSinkMapper;
//    private RedisCommand redisCommand;
//    private FlinkJedisConfigBase flinkJedisConfigBase;
//    private RedisCommandsContainer redisCommandsContainer;
//
//    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper) {
//        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
//        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
//        Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");
//        this.flinkJedisConfigBase = flinkJedisConfigBase;
//        this.redisSinkMapper = redisSinkMapper;
//        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
//        this.redisCommand = redisCommandDescription.getCommand();
//        this.additionalKey = redisCommandDescription.getAdditionalKey();
//    }
//
//    public void invoke(IN input) throws Exception {
//        String key = this.redisSinkMapper.getKeyFromData(input);
//        String value = this.redisSinkMapper.getValueFromData(input);
//        switch(this.redisCommand) {
//        case RPUSH:
//            this.redisCommandsContainer.rpush(key, value);
//            break;
//        case LPUSH:
//            this.redisCommandsContainer.lpush(key, value);
//            break;
//        case SADD:
//            this.redisCommandsContainer.sadd(key, value);
//            break;
//        case SET:
//            this.redisCommandsContainer.set(key, value);
//            break;
//        case PFADD:
//            this.redisCommandsContainer.pfadd(key, value);
//            break;
//        case PUBLISH:
//            this.redisCommandsContainer.publish(key, value);
//            break;
//        case ZADD:
//            this.redisCommandsContainer.zadd(this.additionalKey, value, key);
//            break;
//        case ZREM:
//            this.redisCommandsContainer.zrem(this.additionalKey, key);
//            break;
//        case HSET:
//            this.redisCommandsContainer.hset(this.additionalKey, key, value);
//            break;
//        default:
//            throw new IllegalArgumentException("Cannot process such data type: " + this.redisCommand);
//        }
//
//    }
//
//    public void open(Configuration parameters) throws Exception {
//        try {
//            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
//            this.redisCommandsContainer.open();
//        } catch (Exception var3) {
//            LOG.error("Redis has not been properly initialized: ", var3);
//            throw var3;
//        }
//    }
//
//    public void close() throws IOException {
//        if (this.redisCommandsContainer != null) {
//            this.redisCommandsContainer.close();
//        }
//
//    }
//}
