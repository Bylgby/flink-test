package com.org.apache.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * created date 2022/3/1 21:40
 * <p>
 *  从文件中读取数据
 * @author martinyuyy
 */
public class SourceFromFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");

        dataStream.print();

        env.execute();
    }
}
