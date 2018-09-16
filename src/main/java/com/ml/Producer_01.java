package com.ml;

/**
 * 启动zookeeper和kafka
 * 1. bin/kafka-server-start.sh config/server.properties 1>/dev/null 2>&1 &
 * 2. bin/kafka-console-consumer.sh --zookeeper bigdata112 --topic first --consumer.config config/consumer.properties
 */

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.log4j.BasicConfigurator;

public class Producer_01 {

    public static void main(String[] args) {

        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "bigdata111:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 分区
//        props.put("partitioner.class", "com.ml.Partitioner_01");

        // log4
//        BasicConfigurator.configure();

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>("one", Integer.toString(i), "hello world-" + i));
        }

        producer.close();
    }
}