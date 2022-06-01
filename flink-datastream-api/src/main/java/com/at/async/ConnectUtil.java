package com.at.async;

import org.apache.flink.api.java.tuple.Tuple4;

import java.sql.*;

/**
 * @create 2022-05-31
 */
public class ConnectUtil {

    /*
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>8.0.29</version>
		</dependency>

     */

    private static Connection connection;


    public static Connection getConnection() {

        if (connection == null) {
            synchronized (Connection.class) {
                if (connection == null) {
                    try {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        connection = DriverManager.getConnection(
                                "jdbc:mysql://hadoop102:3306/gmall_report?characterEncoding=utf-8&useSSL=false",
                                "root",
                                "root"
                        );
                    } catch (ClassNotFoundException | SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return connection;

    }

    public static UserInfo query(String sql) {

        System.out.println("execute sql : " + sql);

        if (connection == null) {
            connection = getConnection();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {

            preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {

                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                int sex = resultSet.getInt("sex");
                String address = resultSet.getString("address");

                return UserInfo.of(name, age, sex, address);

            }

        } catch (SQLException t1) {
            t1.printStackTrace();
        } finally {

            try {
                preparedStatement.close();
                resultSet.close();
            } catch (SQLException t2) {
                t2.printStackTrace();
            }

        }

        return null;

    }

    public static void main(String[] args) {

        // select name,age,sex,address rule_table where name = 'Liz'
        UserInfo query = query("select * from rule_table where name='Mary'");

        System.out.println(query);

    }


}
