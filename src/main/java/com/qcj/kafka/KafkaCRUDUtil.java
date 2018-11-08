package com.qcj.kafka;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.junit.Test;

/**
 *  ZkUtils帮助类
 * 描述： 	Topic增删改查
 */
public class KafkaCRUDUtil {
	
	public static void main(String[] args) {
		
//		createTopic();
//		deleteTopic();
//		listTopic();
//		alterTopic();
	}

	/**
	 * 创建一个单分区单副本名为t1的topic
	 */
	@Test
	public void createTopic() {
		ZkUtils zkUtils = ZkUtils.apply("hadoop1:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}

	/**
	 * 删除topic 't1'
	 */
	@Test
	public void deleteTopic() {
		ZkUtils zkUtils = ZkUtils.apply("hadoop1:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		AdminUtils.deleteTopic(zkUtils, "t1");
		zkUtils.close();
	}

	/**
	 * 修改topic
	 */
	@Test
	public void alterTopic(){
		ZkUtils zkUtils = ZkUtils.apply("hadoop1:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "hadoop1");
		// 增加topic级别属性
		props.put("min.cleanable.dirty.ratio", "0.4");
		// 删除topic级别属性
		props.remove("max.message.bytes");
		// 修改topic 'test'的属性
		AdminUtils.changeTopicConfig(zkUtils, "t1", props);
		zkUtils.close();
	}

	/**
	 * 列出所有的Topic
	 */
	@Test
	public void listTopic() {
		ZkUtils zkUtils = ZkUtils.apply("hadoop1:2181,hadoop2:2181,hadoop3:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 获取topic 'test'的topic属性属性
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "hadoop1");
		// 查询topic-level属性
		Iterator it = props.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			System.out.println(key + " = " + value);
		}
		zkUtils.close();
	}
}
