package com.qcj.test;

import java.io.InputStream;
import java.util.Properties;

/**
 * 测试从properties中读写数据
 */
public class TestProps {

	public static void main(String[] args) throws Exception {
		//输入流
		InputStream in = TestProps.class.getClassLoader().getResourceAsStream("test.properties");
		//加载配置文件
		Properties properties = new Properties();
		//从输入流中读取属性列表（键和元素对）
		properties.load(in);
		//从properties中读数据
		System.out.println(properties.get("username"));
	}
}
