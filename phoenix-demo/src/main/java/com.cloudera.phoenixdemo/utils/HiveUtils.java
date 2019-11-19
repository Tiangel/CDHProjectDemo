package com.cloudera.phoenixdemo.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
@Component
public class HiveUtils {
    @Value("${hive.driverName}")
    private String driverName;
    @Value("${hive.url}")
    private String url;
    @Value("${hive.userNmae}")
    private String userNmae;
    @Value("${hive.password}")
    private String password;

    /**
     * 执行修改操作
     *
     * @param sql
     * @return
     */
    public boolean execSql(String sql) {
        boolean result = false;
        log.info("开始执行hivesql:" + sql);
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        // get connection
        Connection con = null;
        try {
            con = DriverManager.
                    getConnection(url, userNmae, password);
        } catch (SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        try {
            Statement stmt = con.createStatement();
            result = stmt.execute(sql);
        } catch (SQLException e) {
            log.info("执行hivesql错误:" + e.getMessage());
            e.printStackTrace();
        }
        log.info("执行hivesql成功:" + result);
        return result;
    }

    /**
     * 异步添加数据入hive
     * @param sinkDate
     * @param sourceData
     * @param dealData
     * @return
     */
    @Async
    public void addPassengerFlowProfile(String sinkDate, String sourceData, String dealData) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("insert into passenger_flow.passenger_flow_profile select ");

        stringBuilder.append("'" + sinkDate + "',");
        stringBuilder.append("'" + sourceData + "',");
        stringBuilder.append("'" + dealData + "'");
        String execSql = stringBuilder.toString();
        //"insert into passenger_flow.passenger_flow_profile select '" + sinkDate + "','" + sourceData + "','" + dealData + "'";
        execSql(execSql);
    }
}
