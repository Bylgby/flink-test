package com.org.apache.api.sink;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * created date 2022/3/5 11:04
 * <p>
 *
 * @author martinyuyy
 */
public class Sink2Fakfa {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka连接配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 使用addSource 方法从自定义数据源中读取数据
        DataStream<String> inputStream = env.addSource(
                // kafka消费者 (topic,序列化类型,连接配置)
                new FlinkKafkaConsumer<String>(
                        "sensor",
                        new SimpleStringSchema(),
                        properties
                )
        );
        inputStream.print();
        // 转换类型
        DataStream<String> outputStream = inputStream.map((MapFunction<String, String>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2])).toString();
        });

        // sink
        outputStream.addSink(
                new FlinkKafkaProducer<>(
                        "hadoop100:9092",
                        "flinkSink",
                        new SimpleStringSchema()
                )
        );

        env.execute();
    }
}
