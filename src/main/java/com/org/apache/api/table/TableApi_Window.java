package com.org.apache.api.table;

import com.org.apache.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * created date 2022/3/15 22:05
 * <p>
 * 窗口函数
 *
 * @author martinyuyy
 */
public class TableApi_Window {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 定义时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置watermark
        DataStream<SensorReading> streamSource = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt").map(
                value -> new SensorReading(
                        value.split(",")[0],
                        Long.parseLong(value.split(",")[1]),
                        Double.parseDouble(value.split(",")[2])
                )
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000;
            }
        });

        // 转换成table
        Table sourceTable = tableEnv.fromDataStream(streamSource, "id, temperature as temp, ts.rowtime");
        // 注册成动态表
        tableEnv.createTemporaryView("sensor", sourceTable);
        // 滚动窗口
        Table tw = sourceTable.window(Tumble.over("1.second").on("ts").as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temp.avg, tw.end");

        tableEnv.toAppendStream(tw, Row.class).print("tw");

        // over窗口  Table API
        Table overTable = sourceTable.window(Over.partitionBy("id").orderBy("ts").as("ow"))
                .select("id, ts, id.count over ow");

        // SQL
        Table sqlQuery = tableEnv.sqlQuery("select id, count(id) over ow " +
                " from sensor " +
                " window ow as (partition by id order by ts rows between 2 preceding and current row)");

        tableEnv.toAppendStream(sqlQuery, Row.class).print("sqlQuery");


        tableEnv.toAppendStream(overTable, Row.class).print("overTable");

        env.execute();
    }
}
