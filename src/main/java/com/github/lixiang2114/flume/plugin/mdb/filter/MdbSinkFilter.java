package com.github.lixiang2114.flume.plugin.mdb.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Louis(LiXiang)
 * @description 自定义Sink过滤器接口规范
 */
public interface MdbSinkFilter {
	/**
	 * 获取数据库名称
	 * @return 索引名称
	 */
	public String getDataBaseName();
	
	/**
	 * 获取集合名称
	 * @return 索引类型
	 */
	public String getCollectionName();
	
	/**
	 * 处理文档记录
	 * @param record 文本记录
	 * @return 文档字典对象
	 */
	public HashMap<String,Object>[] doFilter(String record);
	
	/**
	 * 获取文档ID字段名
	 * @return ID字段名
	 */
	default public String getDocId(){return null;}
	
	/**
	 * 获取登录密码
	 * @return 密码
	 */
	default public String getPassword(){return null;}
	
	/**
	 * 获取登录用户名
	 * @return 用户名
	 */
	default public String getUsername(){return null;}
	
	/**
	 * 过滤器上下文配置(可选实现)
	 * @param config 配置
	 */
	default public void filterConfig(Properties properties){}
	
	/**
	 * 设置MongoDB客户端
	 * @return ID字段名
	 */
	default public void setMongoClient(Object mongoClient){}
	
	/**
	 * 插件上下文配置(可选实现)
	 * @param config 配置
	 */
	default public void pluginConfig(Map<String,String> config){}
}
