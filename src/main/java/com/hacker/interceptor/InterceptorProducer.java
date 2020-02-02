package com.hacker.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description :
 */
@SuppressWarnings("all")
public class InterceptorProducer {

    public static void main(String[] args) {
        //1.创建配置文件
        Properties properties = new Properties();
        //定义配置信息，其他没有添加的配置为默认配置数据
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        //自定义拦截链
        ArrayList<String> interceptorList = new ArrayList<String>();
        interceptorList.add("com.hacker.interceptor.TimeInterceptor");
        interceptorList.add("com.hacker.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptorList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //发送数据
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("topic","测试数据001"));
        }
        kafkaProducer.close();
    }
}
