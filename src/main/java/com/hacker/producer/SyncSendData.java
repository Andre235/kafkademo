package com.hacker.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description : 生产者同步发送数据
 * 同步发送概念：
 *      一条数据发送出去后，阻塞当前线程，直至返回ack，然后发送下一条数据
 * 同步发送原理实现：
 *      send方法返回的是一个Future对象，根据Future对象的特征，可以实现同步发送效果(调用get()方法即可)
 */
@SuppressWarnings("all")
public class SyncSendData {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建配置文件
        Properties properties = new Properties();
        //定义配置信息，其他没有添加的配置为默认配置数据
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("topic01","测试数据")).get();
        }
        kafkaProducer.close();
    }
}
