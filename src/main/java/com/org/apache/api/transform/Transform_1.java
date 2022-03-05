package com.org.apache.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * created date 2022/3/3 21:31
 * <p>
 * 基本转换算子
 *  map flatMap filter
 * @author martinyuyy
 */
public class Transform_1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        // 1. map算子
        DataStream<Integer> mapS = dataStream.map((MapFunction<String, Integer>) String::length);

        // 2. flatmap 按逗号分隔
        DataStream<String> flatMap = dataStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] datas = value.split(",");
            for (String data : datas) {
                out.collect(data);
            }
        });

        // 3. filter
        DataStream<String> filter = dataStream.filter((FilterFunction<String>) value -> value.contains("b"));


        dataStream.print();

        env.execute();
    }
}
