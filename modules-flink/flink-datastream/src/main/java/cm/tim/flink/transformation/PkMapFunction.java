package cm.tim.flink.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * 实现生命周期方法
 */
public class PkMapFunction extends RichMapFunction<String, AccessLog> {

    /**
     * 每条数据转换的时候都会调用一次
     */
    @Override
    public AccessLog map(String s) throws Exception {
        System.out.println("map()");
        String[] split = s.trim().split(",");
        if (split.length < 3) return null;
        Long time = Long.parseLong(split[0]);
        String domain = split[1];
        Double traffic = Double.parseDouble(split[2]);
        return new AccessLog(time, domain, traffic);
    }

    /**
     * 执行次数跟并行度相关
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open()");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close()");
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }
}
