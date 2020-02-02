package com.hacker.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description : 时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    /**
     * @param record 在record的value头部添加时间戳
     * @return
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(),record.partition(),
                record.timestamp(), record.key(), System.currentTimeMillis()+","+record.value());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
