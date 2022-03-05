package com.org.apache.api.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * created date 2022/3/5 11:55
 * <p>
 *
 * @author martinyuyy
 */
public class Sink2Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");
        dataStream.addSink(new MysqlSink());
        env.execute();
    }

    public static class MysqlSink extends RichSinkFunction<String> {

        // 声明连接和预编译语句
        Connection conn = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/t_database", "root", "root");
            insert = conn.prepareStatement("insert into t_sensor (id , temp) values ( ? , ?)");
            update = conn.prepareStatement("update t_sensor set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            conn.close();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 执行sql
            update.setDouble(1, Double.parseDouble(value));
            update.setInt(2, Integer.parseInt(value));
            update.execute();
            if (update.getUpdateCount() == 0) {
                insert.setDouble(2, Double.parseDouble(value));
                insert.setInt(1, Integer.parseInt(value));
                insert.execute();
            }
        }
    }
}
