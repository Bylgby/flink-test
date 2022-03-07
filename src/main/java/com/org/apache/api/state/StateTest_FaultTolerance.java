package com.org.apache.api.state;

import com.org.apache.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * created date 2022/3/6 11:46
 * <p>
 * flink 容错机制
 * <p>
 * StateBackend  -- 状态后端
 * CheckPoint    -- 检查点
 *
 * @author martinyuyy
 */
public class StateTest_FaultTolerance {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置并行度
        env.setParallelism(1);
        // 设置时间属性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置状态后端
        // 状态后端保存在JobManager
        env.setStateBackend( new MemoryStateBackend());
        // 状态后端保存在文件系统
        env.setStateBackend( new FsStateBackend("hdfs:///"));
        // 状态后端保存在RocksDb 需要导入jar包
        env.setStateBackend( new RocksDBStateBackend(""));

        // 设置checkpoint
        // 检查点间隔时间毫秒
        env.enableCheckpointing(1000);

        // 检查点高级选项
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 重启策略配置
        // 固定延迟重启  重启次数， 重启间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        // 失败率重启    失败次数，总时间段，每次重启间隔时间，  如下表示10分钟内重启3次，每次启动间隔1分钟
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);


        streamSource
                // 将字符串转换成bean
                .map(
                        value -> new SensorReading(
                                value.split(",")[0],
                                Long.parseLong(value.split(",")[1]),
                                Double.parseDouble(value.split(",")[2])
                        )
                );


        env.execute();

    }

}
