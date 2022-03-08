package com.org.apache.api.table;

import com.org.apache.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * created date 2022/3/8 21:54
 * <p>
 *
 * @author martinyuyy
 */
public class TableApiTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<SensorReading> operator = streamSource
                // 将字符串转换成bean
                .map(
                        value -> new SensorReading(
                                value.split(",")[0],
                                Long.parseLong(value.split(",")[1]),
                                Double.parseDouble(value.split(",")[2])
                        )
                );
        // 创建环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 基于dataStream创建一张表
        Table table = tableEnv.fromDataStream(operator);
        Table resultTable = table.select("id").where("id = 'sensor_1'");
        tableEnv.toAppendStream(resultTable, Row.class).print("sensor_1");

        // 执行sql
        tableEnv.createTemporaryView("sensor", table);
        String sql = "select * from sensor where id = 'sensor_2'";
        Table resultSql = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(resultSql, Row.class).print("sensor_2");

        env.execute();
    }
}
