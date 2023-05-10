package com.pcitc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.pcitc.utils.DateFormatUtil;
import com.pcitc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//数据流：web/app->日志服务器(.log)->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)->FlinkApp->Kafka(DWD)
//程  序：Mock(lg.sh)->Flume(f1)->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUniqueVisitorDetail->Kafka(ZK)
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception{

        //1,获取执行环境
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

        //2，读取Kafka页面日志主题，创建流

        String topic = "dwd_traffic_page_log";
        String groupId = "UniqueVisitorDetail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        //3，过滤上一跳不为null的数据，并将每行数据转换为json对象

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    //获取上一跳页面id
                    String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (last_page_id == null) {
                        collector.collect(jsonObject);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
            }
        });
        //4，按照mid分组

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //5，使用状态变成实现按照mid的去重工作

        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //设置状态的TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stateDescriptor.enableTimeToLive(ttlConfig);


                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //获取状态数据&当前数据中的时间并转化为日期
                String lastDate = lastVisitState.value();
                String currDate = DateFormatUtil.toDate(jsonObject.getLong("ts"));

                if (lastDate == null || !lastDate.equals(currDate)) {
                    lastVisitState.update(currDate);
                    return true;
                } else return false;
            }
        });

        //6将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>>");
        //SingleOutputStreamOperator<String> map = uvDS.map(json -> json.toJSONString());

        SingleOutputStreamOperator<String> map = uvDS.map(JSONAware::toJSONString);
        map.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));




        //7,启动
        env.execute();

    }
}
