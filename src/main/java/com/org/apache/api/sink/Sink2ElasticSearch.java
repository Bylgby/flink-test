package com.org.apache.api.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * created date 2022/3/5 11:41
 * <p>
 *
 * @author martinyuyy
 */
public class Sink2ElasticSearch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("D:\\flink-test\\src\\main\\resources\\Sensor.txt");
        ArrayList<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<>(hosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                // 数据源
                HashMap<String, String> dataSource = new HashMap<>();
                dataSource.put("id", s.substring(1));
                dataSource.put("name", s.substring(2));

                // 包装请求
                IndexRequest request = Requests.indexRequest()
                        .index("sensor")
                        .type("sensor")
                        .source(dataSource);
                // 发送请求
                requestIndexer.add(request);
            }
        }).build());
        env.execute();
    }
}
