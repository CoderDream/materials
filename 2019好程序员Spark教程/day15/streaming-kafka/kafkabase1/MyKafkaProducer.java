package com.qf.kafkatest.kafkabase1;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;


public class MyKafkaProducer {
    public static void main(String[] args) {
        String mytopic = "mytest1";
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "node01:9092,node02:9092,node03:9092");
        props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));

        for (int index = 0; index < 50; index++) {
            producer.send(new KeyedMessage<String, String>(mytopic, index + "", UUID.randomUUID() + ""));
        }
    }
}