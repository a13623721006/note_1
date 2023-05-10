package com.pcitc.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pcitc.bean.TableProcess;
import com.pcitc.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        //1,获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String after = jsonObject.getString("after");


        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
        //System.out.println(tableProcess.toString());
        //2,校验并建表

        checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns()
        ,tableProcess.getSinkPk(),tableProcess.getSinkExtend()
        );

        //3,写出状态，并广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        broadcastState.put(tableProcess.getSourceTable(),tableProcess);

    }





    /**
     * 校验并建表：create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
     * @param sinkTable  phoenix表名
     * @param sinkColumns phoenix字段名
     * @param sinkPk      phoenix主键名
     * @param sinkExtend  phoenix扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        try {
            //处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }
            //拼接sql
            StringBuilder createTableSql
                    = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i <columns.length; i++) {
                //取出字段
                String column = columns[i];

                //判断是否为主键
                if (sinkPk.equals(column)){
                    createTableSql.append(column).append(" varchar primary key ");
                }else {
                    createTableSql.append(column).append(" varchar ");
                }
                //判断是否为最后一个字段
                if (i<columns.length-1){
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);


            //编译sql
            System.out.println("建表语句为："+createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行sql，建表
            preparedStatement.execute();


        } catch (SQLException e) {
            throw new RuntimeException("建表失败："+sinkTable);
        }finally {

            if (preparedStatement != null){
                //释放资源
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }


    }


    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);


        if (tableProcess != null){
            //过滤字段
            filterColumn(jsonObject.getJSONObject("data"),tableProcess.getSinkColumns());
            //补充SinkTable并写出到流中
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            collector.collect(jsonObject);

        }else {
            System.out.println("找不到对应的key:"+table);
        }
    }

    /**
     * 过滤字段的方法
     * @param data  主表的字段
     * @param sinkColumns 配置表里的字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);


        Set<Map.Entry<String, Object>> entries = data.entrySet();
/*        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }*/

        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }


}
