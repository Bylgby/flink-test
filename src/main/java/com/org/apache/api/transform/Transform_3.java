package com.org.apache.api.transform;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * created date 2022/3/3 22:11
 * <p>
 * reduce 算子
 *
 * @author martinyuyy
 */
public class Transform_3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        // 转换类型
        DataStream<SensorReading> mapStream = dataStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.getLong(fields[1]), Double.parseDouble(fields[2]));
        });

        KeyedStream<SensorReading, Integer> keyedStream = mapStream.keyBy(value -> value.getId().length());

        keyedStream.reduce((curState, newData) ->
                new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature())));

        dataStream.print();

        env.execute();
    }
}
