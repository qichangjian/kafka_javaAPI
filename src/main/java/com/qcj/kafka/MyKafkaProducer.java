package com.qcj.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 通过这个KafkaProducerOps向Kafka topic中生产相关的数据
 *
 * 在linux节点下启动消费者：
 * kafka-console-consumer.sh --zookeeper hadoop1:2181,hadoop2:2181,hadoop3:2181 --from-beginning --topic kafka_test11
 */
public class MyKafkaProducer {
	
	public static void main(String[] args) throws IOException {
		
		/**
		 * 专门加载配置文件
		 * 配置文件的格式：
		 * key=value
		 * 在代码中要尽量减少硬编码
		 * 不要将代码写死，要可配置化
		 */
		Properties properties = new Properties();
		InputStream in = MyKafkaProducer.class.getClassLoader().getResourceAsStream("producer.properties");
		properties.load(in);
//		properties.setProperty("", "")
		
		/**
		 * 两个泛型参数
		 * 第一个泛型参数：指的就是kafka中一条记录key的类型
		 * 第二个泛型参数：指的就是kafka中一条记录value的类型
		 */
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		String topic = properties.getProperty("producer.topic");
//		String topic = KAFKA_TOPIC;
		String key = "key";
		String value = "我爱北京天安门";
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
		
		/**
		 * 发送消息
		 */
		producer.send(producerRecord);
		producer.close();
	}
}