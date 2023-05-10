package com.pcitc.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.pcitc.utils.DruidDSUtil;
import com.pcitc.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {


    private  DruidDataSource druidDataSource = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    /**
     *
     * @param value
     * @param context
     * @throws Exception
     */

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

/*        try {
            PhoenixUtil.upsertValues(connection,sinkTable,data);
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
        PhoenixUtil.upsertValues(connection,sinkTable,data);
        //归还连接
        connection.close();


    }
}
