package com.org.apache.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * created date 2022/2/27 14:40
 * <p>
 *
 * @author martinyuyy
 */
public class KafkaConfig {

    private static Properties properties;

    public static KafkaProducer getProducer(String kafkaHost, String kafkaPort, String keySer, String valueSer) {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer);

        return new KafkaProducer<String, String>(properties);
    }

    public static KafkaConsumer getConsumer(String kafkaHost, String kafkaPort, String consumerGroup, boolean autoCommit, String keyDeSer, String valueDeSer) {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSer);
        // 设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        if (autoCommit) {
            // 自动提交offset
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
        }
        return new KafkaConsumer<String, String>(properties);
    }

}
