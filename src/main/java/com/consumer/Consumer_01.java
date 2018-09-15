package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer_01 {
    public static void main(String[] args) {
        // 配置信息
        Properties props = new Properties();
        // kafka集群
        props.put("bootstrap.servers", "bigdata111:9092");
        // 消费者组
        props.put("group.id", "test");
        // 自动提交(偏移值) offset
        props.put("enable.auto.commit", "true");
        // 提交延时
        props.put("auto.commit.interval.ms", "1000");
        // KV 反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 一个消费者可以消费多个topic，但同一个组内只能消费一个topic
        consumer.subscribe(Arrays.asList("first", "second", "bigdata"));

        while(true) {
            // 拉取数据(多条数据)
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            // 打印数据信息
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("topic: " + consumerRecord.topic() + " -- " + "partition: " + consumerRecord.partition() + " -- " + "offset: " + consumerRecord.offset() + " -- " + "value: " + consumerRecord.value());
            }
        }

/*        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records)
              System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }*/
    }
}
