package com.org.apache.api.source;

import com.org.apache.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * created date 2022/3/1 22:40
 * <p>
 * 自定义数据源
 *
 * @author martinyuyy
 */
public class SourceFromSourceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用addSource 方法从自定义数据源中读取数据
        DataStream<SensorReading> dataStream = env.addSource(
                new MySensor()
        );
        dataStream.print();
        env.execute();
    }

    public static class MySensor implements SourceFunction<SensorReading> {

        private boolean flag = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (flag) {
                // doSomething
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
