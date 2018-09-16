package com.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

// topic one 中获取到的数据，并没有作反序列化处理，所以数据格式为字节数组
public class LogProcessor_01 implements Processor<byte[], byte[]> {

    private ProcessorContext processorContext = null;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        String line = new String(value);
        if (line.contains("-")) {
            String[] split_ = line.split("-");
            line = split_[1];
        }
        // 转化为字节数组传输
        processorContext.forward(key, line.getBytes());
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
