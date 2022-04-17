package cm.tim.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        test_map(env);
//        test_filter(env);
//        test_keyBy(env);
//        test_reduce(env);
//        test_sink(env);
        test_richMap(env);
        env.execute("TransformationApp");
    }

    public static void test_richMap(StreamExecutionEnvironment env){
        env.setParallelism(2); // 并行度设置为2，open就只会调用两次
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<AccessLog> map = source.map(new PkMapFunction());
        map.print();
    }

    public static void test_sink(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        System.out.println("source: " + source.getParallelism());

        /*
         * print() 源码
         * PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
         * return addSink(printFunction).name("Print to Std. Out");
         */
        source.print().setParallelism(1); // 设置并行度为1

        source.printToErr();
        // 参数就是加个前缀
        source.print(" prefix ");
    }

    public static void test_keyBy(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<AccessLog> map = source.map((MapFunction<String, AccessLog>) s -> {
            String[] split = s.trim().split(",");
            if (split.length < 3) return null;
            Long time = Long.parseLong(split[0]);
            String domain = split[1];
            Double traffic = Double.parseDouble(split[2]);
            return new AccessLog(time, domain, traffic);
        });
        // 按照domain分组，对traffic求和
//        map.keyBy("domain").sum("traffic").print(); // 过时写法

        map.keyBy(new KeySelector<AccessLog, String>() {
            @Override
            public String getKey(AccessLog accessLog) throws Exception {
                return accessLog.getDomain();
            }
        }).sum("traffic").print();

        // Lambda 写法，其实用Scala更爽
        map.keyBy((KeySelector<AccessLog, String>) AccessLog::getDomain).sum("traffic").print();
    }

    public static void test_filter(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<AccessLog> map = source.map((MapFunction<String, AccessLog>) s -> {
            String[] split = s.trim().split(",");
            if (split.length < 3) return null;
            Long time = Long.parseLong(split[0]);
            String domain = split[1];
            Double traffic = Double.parseDouble(split[2]);
            return new AccessLog(time, domain, traffic);
        });

        map.filter(new FilterFunction<AccessLog>() {
            @Override
            public boolean filter(AccessLog accessLog) throws Exception {
                return accessLog.getTraffic() > 4000;
            }
        }).print();
    }

    /**
     * 读进来的数据是一行行的，也是字符串类型
     *
     * 将map算子对应的函数作用到DataStream，产生新的DataStream
     * map会作用到已有的DataStream这个数据集的每一个元素上
     */
    public static void test_map(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<AccessLog> map = source.map((MapFunction<String, AccessLog>) s -> {
            String[] split = s.trim().split(",");
            if (split.length < 3) return null;
            Long time = Long.parseLong(split[0]);
            String domain = split[1];
            Double traffic = Double.parseDouble(split[2]);
            return new AccessLog(time, domain, traffic);
        });
        map.print();
        /*
         * 6> AccessLog{time=202512120010, domain='cn.tim', traffic=1000.0}
         * 1> AccessLog{time=202512120010, domain='cn.tim', traffic=3000.0}
         * 3> AccessLog{time=202512120010, domain='com.tim', traffic=7000.0}
         * 8> AccessLog{time=202512120010, domain='com.tim', traffic=6000.0}
         * 12> AccessLog{time=202512120010, domain='cn.tim', traffic=2000.0}
         * 9> AccessLog{time=202512120010, domain='cn.xx', traffic=5000.0}
         * 4> AccessLog{time=202512120010, domain='com.tim', traffic=4000.0}
         */
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);

        env.fromCollection(list)
                .map((MapFunction<Integer, Integer>) integer -> integer * 2)
                .print();
//        3> 4
//        4> 6
//        2> 2
    }
}
