package com.org.apache.wc;

import com.org.apache.common.KafkaConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * created date 2022/2/27 14:39
 * <p>
 *
 * @author martinyuyy
 */
public class Kafka2Flink {

    public static void main(String[] args) {
        // 1. 初始化Kafka
        KafkaProducer producer = KafkaConfig.getProducer(
                "hadoop100",
                "9092",
                "org.apache.kafka.common.serialization.StringSerializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 初始化flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 从kafka消费数据,并做 worldcount




    }
}
