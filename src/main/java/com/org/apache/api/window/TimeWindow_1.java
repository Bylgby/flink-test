package com.org.apache.api.window;

import com.org.apache.api.sink.Sink2Mysql;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.OutputTag;

/**
 * created date 2022/3/5 16:32
 * <p>
 *  窗口
 * @author martinyuyy
 */
public class TimeWindow_1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间语义为 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");
        KeyedStream<String, Integer> keyedStream = dataStream.keyBy(String::hashCode);
        keyedStream
                // 会话窗口
                //.window(EventTimeSessionWindows.withGap(Time.milliseconds(1000)))
                // 计数窗口, 1个参数为滚动窗口, 两个参数是滑动窗口
                // .countWindow(1)
                // 时间窗口, 1个参数为滚动窗口, 两个参数是滑动窗口
                .timeWindow(Time.minutes(3));
        // 窗口函数分类
        // 增量窗口处理 每来一条数据处理一条,只保存最新状态;
        keyedStream.countWindow(10).aggregate(new AggregateFunction<String, Object, Object>() {
            @Override
            public Object createAccumulator() {
                return null;
            }

            @Override
            public Object add(String value, Object accumulator) {
                return null;
            }

            @Override
            public Object getResult(Object accumulator) {
                return null;
            }

            @Override
            public Object merge(Object a, Object b) {
                return null;
            }
        });

        // 全量窗口数据处理,先收集数据等到窗口期再统一处理
        keyedStream.timeWindow(Time.hours(1)).apply(new WindowFunction<String, Integer, Integer, TimeWindow>() {
            @Override
            public void apply(Integer integer, TimeWindow window, Iterable<String> input, Collector<Integer> out) throws Exception {
                out.collect(IteratorUtils.toList(input.iterator()).size());
            }
        });

        // 迟到数据处理
        SingleOutputStreamOperator<String> res = keyedStream.timeWindow(Time.hours(1))
                // 1分钟内窗口不关闭,等待数据进来再进行计算
                .allowedLateness(Time.minutes(1))
                // 侧输出流, 标识迟到数据
                .sideOutputLateData(new OutputTag<String>("late"))
                .sum("sum");
        // 获取侧输出流
        res.getSideOutput((new OutputTag<String>("late")));
        env.execute();
    }
}
