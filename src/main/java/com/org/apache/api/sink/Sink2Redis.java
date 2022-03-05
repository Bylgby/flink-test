package com.org.apache.api.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * created date 2022/3/5 11:28
 * <p>
 *
 * @author martinyuyy
 */
public class Sink2Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        dataStream.addSink( new RedisSink<>(jedisPoolConfig, new RedisMapper<String>() {

            //定义redis指令
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"sensor");
            }

            @Override
            public String getKeyFromData(String data) {
                return data.substring(3);
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        env.execute();
    }
}
