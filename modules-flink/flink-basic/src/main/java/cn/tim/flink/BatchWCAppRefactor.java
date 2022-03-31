package cn.tim.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 离线计算 —— 词频统计
 * 重构版本 —— 抽取匿名算子
 */
public class BatchWCAppRefactor {
    public static void main(String[] args) throws Exception {
        // 离线计算环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读数据
        DataSource<String> source = env.readTextFile("data/word.data");

        // 设置算子、过滤
        source.flatMap(new WcFlatMapFunction())
                .filter(new WcNullStrFilterFunc())
                .map(new WcSumMapFunction()).groupBy(0)
                .sum(1)
                .print();

        /*
         * (flink,9)
         * (world,3)
         * (hello,3)
         */
        // 批处理无需exec
    }

    static class WcFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] words = s.split(",");
            for(String word: words) {
                collector.collect(word.toLowerCase().trim());
            }
        }
    }

    static class WcNullStrFilterFunc implements FilterFunction<String> {
        @Override
        public boolean filter(String s) throws Exception {
            return StringUtils.isNotEmpty(s);
        }
    }

    static class WcSumMapFunction implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            return new Tuple2<>(s, 1);
        }
    }
}
