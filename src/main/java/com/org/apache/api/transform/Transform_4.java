package com.org.apache.api.transform;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * created date 2022/3/3 22:24
 * <p>
 * 多流转换 split select
 *
 * @author martinyuyy
 */
public class Transform_4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        // 转换类型
        DataStream<SensorReading> mapStream = dataStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        mapStream.print("map");

        // 分流,按照温度值分流
        SplitStream<SensorReading> splitStream = mapStream.split(value -> value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");

        high.print("high");
        low.print("low");
        env.execute();
    }
}
