package cn.tim.flink.sink;
import cn.tim.flink.transformation.AccessLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class SinkApp {
    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        toMySQL(env);
        env.execute("SinkApp");
    }

    public static void toMySQL(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.31.86").build();

        SingleOutputStreamOperator<AccessLog> map = source.map((MapFunction<String, AccessLog>) s -> {
            String[] split = s.trim().split(",");
            if (split.length < 3) return null;
            Long time = Long.parseLong(split[0]);
            String domain = split[1];
            Double traffic = Double.parseDouble(split[2]);
            return new AccessLog(time, domain, traffic);
        });

        SingleOutputStreamOperator<AccessLog> traffic = map.keyBy((KeySelector<AccessLog, String>)
                AccessLog::getDomain).sum("traffic");
        traffic.print();

        // 数据写回 MySQL
        traffic.map(new MapFunction<AccessLog, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(AccessLog value) throws Exception {
                return Tuple2.of(value.getDomain(), value.getTraffic());
            }
        })

                .addSink(new PkMySQLSink());
//        .addSink(new RedisSink<Tuple2<String, Double>>(conf, new PkRedisSink()));
    }
}
