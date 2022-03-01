package com.org.apache.api.source;

import com.org.apache.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * created date 2022/3/1 21:25
 * <p>
 * 从集合中获取数据
 *
 * @author martinyuyy
 */
public class SourceFromCollection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        // 从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5, 65, 6);

        dataStream.print("object");
        integerDataStream.print("integer");

        env.execute("local_test");
    }
}
