package cn.tim.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基于 Flink 流式处理 —— 词频统计
 * 结合 Linux nc命令（功能强大的网络工具）作为 socket 数据流来源
 * nc -lk 9527
 * hello,hello,world,flink,flink,hello
 */
public class SteamingWCApp {
    public static void main(String[] args) throws Exception {
        // 上下文 -> 流处理的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 对接数据源，对接 Socket数据
        DataStreamSource<String> source = env.socketTextStream("192.168.31.32", 9527);

        // 业务逻辑处理
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] words = s.split(",");
                for(String word: words) {
                    out.collect(word.toLowerCase().trim());
                }
            }
        }).filter(new FilterFunction<String>() { // 空字符串过滤
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNotEmpty(s);
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        })
        // .keyBy(0).sum(1) // 已过时：第一个位置做keyBy -> 0, 对第二个位置做求和操作 -> 1
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
        .sum(1)
        .print();
        
        env.execute("SteamingWCApp");
        /*
         * 7> (world,1)
         * 4> (hello,1)
         * 10> (flink,1)
         * 10> (flink,2)
         * 4> (hello,2)
         * 4> (hello,3)
         */
    }
}
