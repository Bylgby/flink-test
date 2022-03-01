package com.org.apache.wc;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * created date 2022/2/27 13:51
 * <p>
 *
 * @author martinyuyy
 */
public class FlinkStreamWorldCount {

    public static void main(String[] args) throws Exception{
        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 读取数据
//        String filePaht = "D:\\flink-test\\src\\main\\resources\\hello.txt";
//        DataStream<String> dataStream = env.readTextFile(filePaht);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        DataStream<String> dataStream = env.socketTextStream(host,port);


        // 处理数据 (nc -l -p 7777)
        DataStream<Tuple2<String, Integer>> operator = dataStream.flatMap(new FinkBatchWorldCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        operator.print();

        // 执行任务
        env.execute();
    }
}
