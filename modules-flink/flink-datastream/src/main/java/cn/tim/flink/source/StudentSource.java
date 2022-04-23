package cn.tim.flink.source;

import cn.tim.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class StudentSource extends RichSourceFunction<Student> {
    Connection connection;

    PreparedStatement preparedStatement;

    @Override
    public void run(SourceContext ctx) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            ctx.collect(new Student(id, name, age));
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtils.getConnection();
        if(connection != null) {
            preparedStatement = connection.prepareStatement("select * from student");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        MySQLUtils.closeConnection(connection, preparedStatement);
    }
}
