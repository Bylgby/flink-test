package com.org.apache.api.state;

import com.org.apache.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
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
        // 检查点保存在JobManager
        env.setStateBackend( new MemoryStateBackend());
        // 检查点保存在文件系统
        env.setStateBackend( new FsStateBackend("hdfs:///"));
        // 检查点保存在RocksDb 需要导入jar包
        env.setStateBackend( new RocksDBStateBackend(""));


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
