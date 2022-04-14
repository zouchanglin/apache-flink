package cn.tim.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceApp {
    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        LocalStreamEnvironment envLocal = StreamExecutionEnvironment.createLocalEnvironment();
//        LocalStreamEnvironment envLocalPar = StreamExecutionEnvironment.createLocalEnvironment(5);
        // withWebUI
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createRemoteEnvironment();

        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        System.out.println(source.getParallelism()); // socketTextStream并行度 -> 1

        // filter时不设置并行度就是和CPU核心数相关
        SingleOutputStreamOperator<String> filterStream = source
                .filter((FilterFunction<String>) s -> !"pk".equals(s))
                        .setParallelism(4);
        System.out.println(filterStream.getParallelism());

        filterStream.print();

        env.execute("SourceApp"); // filter 并行度 -> 12
    }
}
