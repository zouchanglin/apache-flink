package cn.tim.flink.source;

import cm.tim.flink.transformation.AccessLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

public class SourceApp {
    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // test_01(env);
//        test_02(env);
        //test_03(env);
        test_source_01(env);

        env.execute("SourceApp");
    }
    private static void test_source_01(StreamExecutionEnvironment env) {
//        DataStreamSource<AccessLog> source = env.addSource(new AccessLogSource());
//        DataStreamSource<AccessLog> source = env.addSource(new AccessLogSource()).setParallelism(2); error
        DataStreamSource<AccessLog> source = env.addSource(new AccessLogSourceV2()).setParallelism(2);
        System.out.println(source.getParallelism());
        source.print();
    }
    private static void test_03(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));

        System.out.println(stream.getParallelism());
        stream.print();
    }

    private static void test_02(StreamExecutionEnvironment env) {
        env.setParallelism(5); // 对于env设置的并行度是全局的并行度
        DataStreamSource<Long> source = env.fromParallelCollection(
                new NumberSequenceIterator(1, 10), Long.class
        );
        source.setParallelism(3); // 对于算子层面的并行度，如果全局设置，以算子的并行度为准

        System.out.println("source: " + source.getParallelism());

        SingleOutputStreamOperator<Long> filter = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong > 5;
            }
        });

        System.out.println("filter: " + filter.getParallelism());

        filter.print();
    }

    private static void test_01(StreamExecutionEnvironment env) {
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
    }
}
