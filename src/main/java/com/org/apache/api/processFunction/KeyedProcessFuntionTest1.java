package com.org.apache.api.processFunction;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * created date 2022/3/6 12:17
 * <p>
 *
 * @author martinyuyy
 */
public class KeyedProcessFuntionTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置并行度
        env.setParallelism(1);
        // 设置时间属性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置状态后端
        // 检查点保存在JobManager
        env.setStateBackend(new MemoryStateBackend());
        // 检查点保存在文件系统
        env.setStateBackend(new FsStateBackend("hdfs:///"));
        // 检查点保存在RocksDb 需要导入jar包
        env.setStateBackend(new RocksDBStateBackend(""));


        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);


        streamSource
                // 将字符串转换成bean
                .map(
                        value -> new SensorReading(
                                value.split(",")[0],
                                Long.parseLong(value.split(",")[1]),
                                Double.parseDouble(value.split(",")[2])
                        )
                )
                .keyBy("id")
                .process(new MyKeyProcessFunction());

        env.execute();

    }

    public static class MyKeyProcessFunction extends KeyedProcessFunction<Tuple, SensorReading, String> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            out.collect(ctx.getCurrentKey().toString());

            //
            ctx.timerService().currentProcessingTime();
            // 注册定时器
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1000L);

            // 侧输出流
            ctx.output(new OutputTag<SensorReading>("side"), value);
        }

        // 触达定时器之后的操作
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }
    }

}
