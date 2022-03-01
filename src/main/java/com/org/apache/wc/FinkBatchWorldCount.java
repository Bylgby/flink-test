package com.org.apache.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * created date 2022/2/27 13:32
 * <p>
 *
 * @author martinyuyy
 */
public class FinkBatchWorldCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        String filePaht = "D:\\flink-test\\src\\main\\resources\\hello.txt";
        DataSource<String> dataSource = env.readTextFile(filePaht);

        // 处理数据
        DataSet<Tuple2<String, Integer>> operator = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照字段位置进行group by
                .sum(1);// 把第二个位置的字段相加
        operator.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] worlds = value.split(" ");
            for (String world : worlds) {
                out.collect(new Tuple2<>(world, 1));
            }
        }
    }
}
