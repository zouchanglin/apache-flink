package cn.tim.flink.sink;

import cn.tim.flink.transformation.AccessLog;
import cn.tim.flink.utils.MySQLUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * domain - traffic
 */
public class PkMySQLSink extends RichSinkFunction<Tuple2<String, Double>> {
    Connection connection;

    PreparedStatement insertPst;
    PreparedStatement updatePst;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = MySQLUtils.getConnection();
        if(connection == null) throw new RuntimeException("MySQL link failed!");
        insertPst = connection.prepareStatement("insert into pk_traffic(domain, traffic) values (?,?)");
        updatePst = connection.prepareStatement("update pk_traffic set traffic = ? where domain = ?");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(insertPst != null) insertPst.close();
        if(updatePst != null) updatePst.close();
        if(connection != null) connection.close();
    }

    @Override
    public void invoke(Tuple2<String, Double> value, Context context) throws Exception {
        System.out.println("===========invoke==========" + value.f0 + "--->" + value.f1);
        updatePst.setDouble(1, value.f1);
        updatePst.setString(2, value.f0);
        updatePst.execute();

       if(updatePst.getUpdateCount() == 0){
           insertPst.setString(1, value.f0);
           insertPst.setDouble(2, value.f1);
           insertPst.execute();
       }
    }
}
