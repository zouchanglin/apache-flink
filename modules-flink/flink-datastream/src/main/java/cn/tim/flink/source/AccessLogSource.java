package cn.tim.flink.source;

import cm.tim.flink.transformation.AccessLog;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessLogSource implements SourceFunction<AccessLog> {
    boolean running = true;
    @Override
    public void run(SourceContext<AccessLog> ctx) throws Exception {
        String[] domains = {"cn.tim", "tim.com", "pk.com"};
        Random random = new Random();
        while (running) {
            for (int i = 0; i < 10; i++) {
                AccessLog accessLog = new AccessLog();
                accessLog.setTime(1234567L);
                accessLog.setDomain(domains[random.nextInt(domains.length)]);
                accessLog.setTraffic(random.nextDouble() + 1_000);
                ctx.collect(accessLog);
            }

            Thread.sleep(5_000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
