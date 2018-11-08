package com.qcj.kafka;

import kafka.admin.TopicCommand;

public class CreateTopic {
	
	public static void main(String[] args) {
		createTopic();
	}
	
	public static void createTopic() {
		/**
		 * 创建一个主题  topic
		 * 
		 * 
			kafka-topics.sh \
			--create \
		    --zookeeper hadoop1:2181,hadoop2:2181,hadoop3:2181\
			--replication-factor 3 \
			--partitions 10 \
			--topic kafka_test
		 */
		
		String[] ops = new String[] {
				"--create", 
				"--zookeeper", 
				"hadoop1:2181,hadoop2:2181,hadoop3:2181",
				"--replication-factor", 
				"3", 
				"--partitions", 
				"10", 
				"--topic", 
				"kafka_test22" };

		/**
		 * 查询所有topic
		 * kafka-topics.sh --list --zookeeper hadoop1:2181,hadoop2:2181,hadoop3:2181
		 */
		String[] ops1 = new String[]{
				"--list",
				"--zookeeper",
				"hadoop1:2181,hadoop2:2181,hadoop3:2181"
		};

		TopicCommand.main(ops);
		//TopicCommand.main(ops1);
	}
}
