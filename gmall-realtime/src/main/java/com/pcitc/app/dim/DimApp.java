package com.pcitc.app.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pcitc.app.func.DimSinkFunction;
import com.pcitc.app.func.TableProcessFunction;
import com.pcitc.bean.TableProcess;
import com.pcitc.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;

import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


//数据流->web/app->nginx->MySql(binlog)->MaxWell->kafka->flinkAPP->Phoenix
//程序 Mock->MySql(binlog)->MaxWell->kafka(ZK)->DimApp(flinkCDC(MySql))->Phoenix(HBase(zk.hdfs))
public class DimApp {

    public static void main(String[] args) throws Exception {
        // 1，获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为kafka主题的分区数

        //开启CheckPoint
        //env.enableCheckpointing(5*60Currently Flink MySql CDC connector only supports MySql whose version is larger or equal to 5.7, but actual is 5.6000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));

        //设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall/ck");
        //System.setProperty("HADOOP_USER_NAME", "root");


        //2，读取kafka topic_db主题数据创建主流
        String topic = "topic_db";
        String groupId = "dim_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //3，过滤掉非json数据以及保留新增、变化以及初始化数据  并将数据转化为json格式（所以用flatmap，两个参数，out参数可转化成json，filter和map不行）
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    //将数据转化为json格式
                    JSONObject jsonObject = JSON.parseObject(s);
                    //获取数据中的操作类型字段
                    String type = jsonObject.getString("type");
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("发现脏数据" + s);
                }
            }
        });
        //4，使用flinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mySqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mySqlSource");

        //mySqlSourceDS.print("mySqlSourceDS");
        //5，将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>
                ("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mySqlSourceDS.broadcast(mapStateDescriptor);


        //6，连接两条流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterJsonObjDS.connect(broadcastStream);


        //7，处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));



        //8，将数据写到Phoenix
        dimDS.print(">>>>>>>>");
        dimDS.addSink(new DimSinkFunction());
        //9,启动任务
        env.execute("DimApp");
    }


}
