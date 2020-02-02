package com.hacker.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description :
 */
public class MyProducer {

    public static void main(String[] args) {

        //1.创建kafka生产者的配置信息
        Properties properties = new Properties();
        //1.1指定kafka连接的集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //1.2ACK应答级别
        properties.put("acks","all");
        //1.3重试次数
        properties.put("retries", 1);
        //批次大小
        properties.put("batch.size",16384);
        //等待时间
        properties.put("linger.ms",1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);
        //将key value进行序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            //每条数据都要封装成一个ProducerRecord对象
            //分区的目的可让消费者采用轮询的方式去不同的分区去取数据
            kafkaProducer.send(new ProducerRecord<String, String>("topic01",2,"key01","value01"));
        }
        kafkaProducer.close();
    }
}
