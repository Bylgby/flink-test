package com.org.apache.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * created date 2022/3/1 21:44
 * <p>
 * 从Kafka读取数据
 *
 * @author martinyuyy
 */
public class SourceFromKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka连接配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 使用addSource 方法从自定义数据源中读取数据
        DataStream<String> dataStream = env.addSource(
                // kafka消费者 (topic,序列化类型,连接配置)
                new FlinkKafkaConsumer011<String>(
                        "sensor",
                        new SimpleStringSchema(),
                        properties
                )
        );
        dataStream.print();
        env.execute();
    }
}
