package com.hacker.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author : Jeffersonnn
 * @date : 2020/2/2
 * @description : 计数拦截器(统计发送成功的数据条数，发送失败的数据条数)
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private static Integer successCount = 0;
    private static Integer failCount = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return null;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            //成功
            successCount++;
        }else{
            //失败
            failCount++;
        }
    }

    public void close() {
        System.out.println("success count "+successCount);
        System.out.println("fail count "+failCount);
    }

    public void configure(Map<String, ?> configs) {

    }
}
