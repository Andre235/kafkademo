package com.hacker.consumer;

import com.hacker.common.CommonPrivate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description :
 *      由于 consumer 在消费过程中可能会出现断电宕机等故障，consumer 恢复后，需要从故
 * 障前的位置的继续消费，所以 consumer 需要实时记录自己消费到了哪个 offset，以便故障恢
 * 复后继续消费。
 *      所以 offset 的维护是 Consumer 消费数据是必须考虑的问题。
 *      Kafka 提供了自动提交 offset 的功能。
 */
@SuppressWarnings("all")
public class MyConsumer {

    public static void main(String[] args) {
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        //是否开启自动提交 offset 功能
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //自动提交 offset 的时间间隔
        consumerProp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //字符串反序列化
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CommonPrivate.STRING_DESERIALIZER);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonPrivate.STRING_DESERIALIZER);

        //消费数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProp);

        //消费者订阅topic
        consumer.subscribe(Arrays.asList("topic"));
        while (true){
            //消费者拉取数据，最大超时时间为100毫秒
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.printf("offset = %d,key = %s,value = %s \n",consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
            }
        }
    }
}
