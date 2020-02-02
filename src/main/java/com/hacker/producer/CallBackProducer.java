package com.hacker.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description : 带回调函数的生产者
 */
public class CallBackProducer {

    public static void main(String[] args) {
        //1.创建配置文件
        Properties properties = new Properties();
        //定义配置信息，其他没有添加的配置为默认配置数据
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //2.创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //3.发送数据(带回调函数发送数据)
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first",0,"测试数据","测试value" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //不出现异常
                    if(exception == null){
                        System.out.println("分区："+metadata.partition()+" offset+"+metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        //4.关闭资源
        kafkaProducer.close();
    }
}
