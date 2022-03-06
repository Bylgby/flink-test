package com.org.apache.api.state;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * created date 2022/3/6 10:55
 * <p>
 * 键值状态管理
 *
 * @author martinyuyy
 */
public class KeyedStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);


        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resStream = streamSource
                // 将字符串转换成bean
                .map(
                        value -> new SensorReading(
                                value.split(",")[0],
                                Long.parseLong(value.split(",")[1]),
                                Double.parseDouble(value.split(",")[2])
                        )
                )
                // 根据ID分组
                .keyBy("id")
                .flatMap(new WarnFumction(10.0));

        resStream.print();

        env.execute();
    }

    public static class WarnFumction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 报警阈值
        private Double threshold;
        // 定义上次的状态
        private ValueState<Double> lastTmp;

        public WarnFumction(Double threshold) {
            this.threshold = threshold;
        }

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTmp = getRuntimeContext().getState(new ValueStateDescriptor<>("old", Double.class));
        }


        // 数据处理
        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastD = lastTmp.value();

            if (lastD != null) {
                Double gap = Math.abs(value.getTemperature() - lastD);
                if (gap >= threshold)
                    out.collect(new Tuple3<>(value.getId(), lastD, value.getTemperature()));
            }

            // 更新最新状态
            lastTmp.update(value.getTemperature());
        }
    }
}
