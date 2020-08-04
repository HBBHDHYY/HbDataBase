package com.hr.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.hr.realtime.countTarget.realTime_exit_station_tatistics_10minutes;
/**
 * HF
 * 2020-06-07 16:41
 * 德鲁伊连接池
 */
public class DataSourceUtil implements Serializable {

    public static String real_product_or_test = "product";   //test,product 只在这里确定生产还是测试
    public static String real_earliest_or_latest = "earliest";  //latest ,earliest,spark streaming 的
    public static String Structuredsteaming_2_mysqlDatabaseName= "sharedb" ;//rate_statistic   sharedb
    public static DataSource dataSource = null;
    public static String real_jdbcUrl = ConfigurationManager.getProperty("Product.jdbc.url");
    public static String real_jdbcUser = ConfigurationManager.getProperty("Product.jdbc.user");
    public static String real_jdbcPassword = ConfigurationManager.getProperty("Product.jdbc.password");
    public static String real_jdbcDriver= ConfigurationManager.getProperty("Product.jdbc.driver");

    static {
        try {
            if(real_product_or_test.equals("test")){
                real_jdbcUrl = ConfigurationManager.getProperty("Test.jdbc.url");
                real_jdbcUser = ConfigurationManager.getProperty("Test.jdbc.user");
                real_jdbcPassword = ConfigurationManager.getProperty("Test.jdbc.password");
                real_jdbcDriver= ConfigurationManager.getProperty("Test.jdbc.driver");
            }
            Properties props = new Properties();
            props.setProperty("url", real_jdbcUrl);
            props.setProperty("username", real_jdbcUser);
            props.setProperty("password", real_jdbcPassword);
            props.setProperty("initialSize", "5"); //初始化大小
            props.setProperty("maxActive", "300"); //最大连接,可能要调整
            props.setProperty("minIdle", "5");  //最小连接,可能要调整
            props.setProperty("maxWait", "60000"); //等待时长
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            props.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            props.setProperty("validationQuery", "select 1");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("testOnBorrow", "false");
            props.setProperty("testOnReturn", "false");
            props.setProperty("keepAlive", "true");
            props.setProperty("phyMaxUseCount", "100000");
            props.setProperty("driverClassName", real_jdbcDriver); //可删可不删,"com.mysql.jdbc.Driver"
            dataSource = DruidDataSourceFactory.createDataSource(props);
            System.out.println("dataSource:"+dataSource);
            System.out.println("real_product_or_test:"+real_product_or_test);
            System.out.println("real_jdbcUrl:"+real_jdbcUrl);
            System.out.println("real_jdbcUser:"+real_jdbcUser);
            System.out.println("real_jdbcPassword:"+real_jdbcPassword);
            System.out.println("real_jdbcDriver:"+real_jdbcDriver);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    //提供获取连接的方法
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 提供关闭资源的方法【connection是归还到连接池】
    // 提供关闭资源的方法 【方法重载】3 dql
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(real_jdbcUrl);
        System.out.println(real_jdbcUser);
        System.out.println(real_jdbcPassword);
        System.out.println(real_jdbcDriver);
        System.out.println(realTime_exit_station_tatistics_10minutes.get_real_product_or_test());

    }

}
