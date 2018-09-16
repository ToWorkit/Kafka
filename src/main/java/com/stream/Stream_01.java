package com.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Stream_01 {
    public static void main(String[] args) {
        // 配置信息
        // 配置信息
        Properties props = new Properties();
        // kafka集群
        props.put("bootstrap.servers", "bigdata111:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logStream");

        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "one")
                .addProcessor("PROCESSOR", (ProcessorSupplier<byte[], byte[]>) LogProcessor_01::new, "SOURCE")
                .addSink("SINK", "two", "PROCESSOR");

        // 创建kafkastream
        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
    }
}
