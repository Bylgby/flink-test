package com.org.apache.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * created date 2022/3/14 20:54
 * <p>
 *  Kafka 也只支持append
 * @author martinyuyy
 */
public class TableApi_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 连接Kafka
        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("flink_input")
                        .property("bootstrap.servers", "hadoop100:9092")
                        .property("zookeeper.connect", "hadoop100:9092")
        )
                .withFormat(new OldCsv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("name", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 数据处理
        Table resTable = tableEnv.sqlQuery("select id,count(id) as cnt from inputTable group by id");

        // 输出Kafka连接信息
        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("flink_outpu")
                        .property("bootstrap.servers", "hadoop100:9092")
                        .property("zookeeper.connect", "hadoop100:9092")
        )
                .withFormat(new OldCsv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("name", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        // sink
        resTable.insertInto("outputTable");

        env.execute();
    }
}
