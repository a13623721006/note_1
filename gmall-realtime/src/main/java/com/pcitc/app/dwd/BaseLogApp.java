package com.pcitc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pcitc.utils.DateFormatUtil;
import com.pcitc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app->日志服务器(.log)->Flume->Kafka(ODS)->FlinkApp->Kafka(DWD)
//程  序：Mock(lg.sh)->Flume(f1)->Kafka(ZK)->BaseLogApp->Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception{
        //1，获取执行环境
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

        //2，消费kafka topic_log主题的数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //3，过滤掉非json格式的数据将每行数据转换为json对象
        //flatmap可以满足需求，但是我们需要用到侧输出流，只能用process
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        //获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>");
        //4，按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //5，使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //状态初始化需要在open里面实现懒加载
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取is_new标记 & ts
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                //将时间戳转化为年月日
                String currDate = DateFormatUtil.toDate(ts);
                //获取状态中的日期  状态中只存储了日期
                String lastValue = lastVisitState.value();

                //判断is_new标记是否为1
                if ("1".equals(isNew)) {
                    if (lastValue == null) {
                        lastVisitState.update(currDate);
                    } else if (!lastValue.equals(currDate)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastValue == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }


                return jsonObject;
            }
        });

        //6，使用侧输出流进行分流处理 页面日志放在主流，启动、曝光、动作、错误放到侧输出流

        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displays") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actions") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //启动和页面是互斥关系，错误和启动和页面是共存关系，页面和（曝光、动作）是包含关系
                //所以说要先处理错误日志，判断是不是启动，不是启动就一定是页面，页面里面再分析曝光和动作。
                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    //将数据写入error侧输出流
                    ctx.output(errorTag, value.toJSONString());
                }
                //移除错误信息
                value.remove("err");

                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    //将数据写入start侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息&页面&时间戳
                    String common = value.getString("common");
                    String page_id = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");


                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据写到侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", page_id);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据写到侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", page_id);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }


                    //start等于空的时候就是页面数据（互斥）
                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("displays");
                    value.remove("action");
                    out.collect(value.toJSONString());

                }
            }
        });

        //7，提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //8，将数据写入不同的主题并打印数据查看
        pageDS.print("pageDS>>>>>>>>");
        startDS.print("startDS>>>>>>");
        displayDS.print("displayDS>>");
        actionDS.print("actionDS>>>>");
        errorDS.print("errorDS>>>>>>");

        // 7.2 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));


        //9，启动任务
        env.execute("BaseLogApp");

    }


}
