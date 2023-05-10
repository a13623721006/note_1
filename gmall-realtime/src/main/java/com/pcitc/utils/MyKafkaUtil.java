package com.pcitc.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop1:9092,hadoop2:9092,hadoop3:9092";

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupID) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);


        return new FlinkKafkaConsumer<String>(topic
                , new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord == null || consumerRecord.value() == null) {
                    return null;
                } else return new String(consumerRecord.value());
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }
                , properties);
    }



    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){

        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic,String defaultTopic){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(defaultTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                if (s == null){
                    return new ProducerRecord<>(topic,"".getBytes());
                }
                return new ProducerRecord<>(topic,s.getBytes());
            }
        },properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
