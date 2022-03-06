package com.org.apache.api.window;

import com.org.apache.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * created date 2022/3/5 18:51
 * <p>
 * Flink 处理乱序数据, waterMark
 *
 * @author martinyuyy
 */
public class WaterMark_1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间语义为 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        SingleOutputStreamOperator<SensorReading> streamOperator = dataStream.flatMap((value, out) -> out.collect(
                new SensorReading(value.split(",")[0],
                        Long.parseLong(value.split(",")[1]),
                        Double.parseDouble(value.split(",")[2])
                )
        ));

        // 设置watermark 和 eventTime 时间戳
        SingleOutputStreamOperator<SensorReading> res = streamOperator.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        env.execute();

    }
}
