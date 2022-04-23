package cn.tim.flink.partitioner;

import cn.tim.flink.source.AccessLogSourceV2;
import cn.tim.flink.transformation.AccessLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionerApp {
    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<AccessLog> source = env.addSource(new AccessLogSourceV2());
        //source.print();
        //System.out.println("=====================================");

        source.map(new MapFunction<AccessLog, Tuple2<String, AccessLog>>() {
            @Override
            public Tuple2<String, AccessLog> map(AccessLog value) throws Exception {
                return Tuple2.of(value.getDomain(), value);
            }
        })
                .partitionCustom(new PkPartitioner(), new KeySelector<Tuple2<String, AccessLog>, String>() {
                    @Override
                    public String getKey(Tuple2<String, AccessLog> value) throws Exception {
                        return value.f0;
                    }
                })
//                .partitionCustom(new PkPartitioner(), 0)
                        .map(new MapFunction<Tuple2<String, AccessLog>, AccessLog>() {
                            @Override
                            public AccessLog map(Tuple2<String, AccessLog> value) throws Exception {
                                System.out.println("current thread id is:" + Thread.currentThread().getId() + ", value is " + value.f1);
                                return value.f1;
                            }
                        }).print();
        env.execute("PartitionerApp");
    }
}
