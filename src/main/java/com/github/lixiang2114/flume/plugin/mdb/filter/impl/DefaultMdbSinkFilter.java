package com.github.lixiang2114.flume.plugin.mdb.filter.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.github.lixiang2114.flume.plugin.mdb.filter.MdbSinkFilter;

/**
 * @author Louis(LiXiang)
 * @description MongoDB-Sink默认过滤器
 */
@SuppressWarnings("unchecked")
public class DefaultMdbSinkFilter implements MdbSinkFilter{
	/**
	 * 文档ID字段名
	 */
	private static  String docId;
	
	/**
	 * 登录MongoDB用户名
	 */
	private static String userName;
	
	/**
	 * 登录MongoDB密码
	 */
	private static String passWord;
	
	/**
	 * 记录字段列表
	 * 按记录行从左到右区分顺序
	 */
	private static String[] fieldList;
	
	/**
	 * 集合名称
	 */
	private static String collectionName;
	
	/**
	 * 数据库名称
	 */
	private static String dataBaseName;
	
	/**
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	private static Pattern fieldSeparator;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

	@Override
	public String getDocId() {
		return docId;
	}
	
	@Override
	public String getPassword() {
		return passWord;
	}

	@Override
	public String getUsername() {
		return userName;
	}

	@Override
	public String getCollectionName() {
		return collectionName;
	}

	@Override
	public String getDataBaseName() {
		return dataBaseName;
	}

	@Override
	public HashMap<String,Object>[] doFilter(String record) {
		HashMap<String,Object> doc=new HashMap<String,Object>();
		String[] fieldValues=fieldSeparator.split(record);
		if(null==fieldList || 0==fieldList.length){
			for(int i=0;i<fieldValues.length;doc.put("field"+i, fieldValues[i]),i++);
		}else if(fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;doc.put(fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<fieldList.length;doc.put(fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;doc.put("field"+i, fieldValues[i]),i++);
		}
		return new HashMap[]{doc};
	}

	@Override
	public void pluginConfig(Map<String, String> config) {
		dataBaseName=getParamValue(config,"dataBaseName", "fpdata");
		collectionName=getParamValue(config,"collectionName", "logger");
		fieldSeparator=Pattern.compile(getParamValue(config,"fieldSeparator","\\s+"));
		
		String fieldListStr=getParamValue(config,"fieldList",null);
		if(null!=fieldListStr){
			String[] fields=COMMA_REGEX.split(fieldListStr);
			fieldList=new String[fields.length];
			for(int i=0;i<fields.length;i++){
				String fieldName=fields[i].trim();
				if(0==fieldName.length()){
					fieldList[i]="field"+i;
					continue;
				}
				fieldList[i]=fieldName;
			}
		}
		
		String docIdStr=config.get("docId");
		if(null!=docIdStr) {
			String tmp=docIdStr.trim();
			if(0!=tmp.length()) docId=tmp;
		}
		
		String passWordStr=config.get("passWord");
		String userNameStr=config.get("userName");
		if(null!=passWordStr && null!=userNameStr) {
			String pass=passWordStr.trim();
			String user=userNameStr.trim();
			if(0!=pass.length() && 0!=user.length()) {
				userName=user;
				passWord=pass;
			}
		}
	}
	
	/**
	 * 获取参数值
	 * @param context Sink插件上下文
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final String getParamValue(Map<String, String> context,String key,String defaultValue){
		String value=context.getOrDefault(key, defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
}
