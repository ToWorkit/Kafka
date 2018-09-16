package com.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int successCount;
    private int errorCount;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCount++;
        } else {
            errorCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功 " + successCount + " , 失败 " + errorCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
