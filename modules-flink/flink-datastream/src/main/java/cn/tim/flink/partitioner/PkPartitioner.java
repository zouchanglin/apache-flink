package cn.tim.flink.partitioner;


import org.apache.flink.api.common.functions.Partitioner;
public class PkPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        System.out.println("numPartitions = " + numPartitions);
        if("cn.tim".equals(key)){
            return 0;
        }else if("tim.com".equals(key)){
            return 1;
        }else {
            return 2;
        }
    }
}
