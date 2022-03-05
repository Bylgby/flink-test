package com.org.apache.api.transform;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * created date 2022/3/5 10:16
 * <p>
 * 合流  connect CoMap CoFlatMap     -> 可以合并两条数据结构不同的流
 * union  -> 两条流数据结构相同可合并
 *
 * @author martinyuyy
 */
public class Transform_5 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        // 转换类型
        DataStream<SensorReading> mapStream = dataStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 分流,按照温度值分流
        SplitStream<SensorReading> splitStream = mapStream.split(value -> value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");

        // 合流
        ConnectedStreams<SensorReading, SensorReading> connectedStreams = high.connect(low);
        DataStream<Object> streamOperator = connectedStreams.flatMap(new CoFlatMapFunction<SensorReading, SensorReading, Object>() {
            // 第一条流的处理方式
            @Override
            public void flatMap1(SensorReading value, Collector<Object> out) throws Exception {
                out.collect(new Tuple2<String, SensorReading>(value.getId() + "第一条流的内容:", value));
            }

            //第二条流的处理方式
            @Override
            public void flatMap2(SensorReading value, Collector<Object> out) throws Exception {
                out.collect(new Tuple1<String>("第二流: " + value.getId()));
            }
        });

        // union 合流

        DataStream<SensorReading> unionStream = low.union(high);

    }
}
