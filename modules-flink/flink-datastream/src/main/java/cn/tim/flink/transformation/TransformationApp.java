package cn.tim.flink.transformation;

import cn.tim.flink.source.Student;
import cn.tim.flink.source.StudentSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
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
//        test_richMap(env);
//        test_union(env);
//        test_coMap(env);
        test_coFlatMap(env);
        env.execute("TransformationApp");
    }

    public static void test_coFlatMap(StreamExecutionEnvironment env){
        DataStreamSource<String> source1 = env.fromElements("a b c", "d e f");
        DataStreamSource<String> source2 = env.fromElements("1,2,3", "4,5,6");
        ConnectedStreams<String, String> connect = source1.connect(source2);
        connect.flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for(String s: split) {
                    out.collect(s);
                }
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for(String s: split) {
                    out.collect(s);
                }
            }
        }).print();
    }
    public static void test_coMap(StreamExecutionEnvironment env){
        DataStreamSource<String> source1 = env.socketTextStream("192.168.31.86", 9527); // String??????
        DataStreamSource<Student> source2 = env.addSource(new StudentSource()); // Student??????
        // ???????????????????????????
        ConnectedStreams<String, Student> connect = source1.connect(source2);
        // source1????????????source2??????????????????????????????
        connect.map(new CoMapFunction<String, Student, String>() {
            // ?????????????????????????????????
            @Override
            public String map1(String value) throws Exception {
                return value + "-CoMap";
            }
            // ?????????????????????????????????
            @Override
            public String map2(Student value) throws Exception {
                return value.getName();
            }
        }).print();
    }
    public static void test_union(StreamExecutionEnvironment env){
//        DataStreamSource<String> source1 = env.socketTextStream("192.168.31.86", 9527);
//        DataStreamSource<String> source2 = env.socketTextStream("192.168.31.86", 9528);
//        DataStream<String> unionSource = source1.union(source2);
//        unionSource.print();

        DataStreamSource<Student> source = env.addSource(new StudentSource());
        source.union(source).print();
    }
    public static void test_richMap(StreamExecutionEnvironment env){
        env.setParallelism(2); // ??????????????????2???open?????????????????????
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<AccessLog> map = source.map(new PkMapFunction());
        map.print();
    }

    public static void test_sink(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9527);
        System.out.println("source: " + source.getParallelism());

        /*
         * print() ??????
         * PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
         * return addSink(printFunction).name("Print to Std. Out");
         */
        source.print().setParallelism(1); // ??????????????????1

        source.printToErr();
        // ????????????????????????
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
        // ??????domain????????????traffic??????
//        map.keyBy("domain").sum("traffic").print(); // ????????????

        map.keyBy(new KeySelector<AccessLog, String>() {
            @Override
            public String getKey(AccessLog accessLog) throws Exception {
                return accessLog.getDomain();
            }
        }).sum("traffic").print();

        // Lambda ??????????????????Scala??????
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
     * ?????????????????????????????????????????????????????????
     *
     * ???map??????????????????????????????DataStream???????????????DataStream
     * map?????????????????????DataStream????????????????????????????????????
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
