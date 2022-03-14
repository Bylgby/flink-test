package com.org.apache.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * created date 2022/3/14 20:25
 * <p>
 *  输出到文件系统时， 无法更新数据，只能append
 * @author martinyuyy
 */
public class FlinkTable_1 {

    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 表的创建

        // 连接文件系统
        tableEnv.connect(new FileSystem().path("hdfs_path"))
                .withFormat( new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("name", DataTypes.DOUBLE())
                )
                .createTemporaryTable("input");

        Table input = tableEnv.from("input");
        // table API
        input.select("id, name")
                .filter("id = 'next'")
                // 聚合计算
                .groupBy("id")
                .select("id, id.count as cnt");
        //Flink Sql
        tableEnv.sqlQuery("select id, name from input where id = 'next'");
        Table cntTable = tableEnv.sqlQuery("select id, count(id) as cnt from input group by id");

        // 输出到文件系统
        tableEnv.connect(new FileSystem().path("out_path"))
                .withFormat( new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("name", DataTypes.DOUBLE())
                )
                .createTemporaryTable("output");
        cntTable.insertInto("output");

        // 转化为DataStream打印
        tableEnv.toRetractStream(cntTable, Row.class).print("cntTable");
        tableEnv.toAppendStream(input, Row.class).print("cntTable");

        env.execute();
    }
}
