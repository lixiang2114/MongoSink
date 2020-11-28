package com.github.lixiang2114.flume.plugin.mdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.bson.Document;

import com.github.lixiang2114.flume.plugin.mdb.util.ClassLoaderUtil;
import com.github.lixiang2114.flume.plugin.mdb.util.TypeUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;

/**
 * @author Louis(LiXiang)
 * @description Elastic-Sink插件
 */
@SuppressWarnings("unchecked")
public class MongoSink extends AbstractSink implements Configurable, BatchSizeSupported{
	/**
	 * 文档主键字段名
	 */
	private static String docId;
	
	/**
	 * 批处理尺寸
	 */
	private static Integer batchSize;
	
	/**
	 * 过滤器名称
	 */
	private static String filterName;
	
	/**
	 * 过滤方法
	 */
	private static Method doFilter;
	
	/**
	 * 过滤器对象
	 */
	private static Object filterObject;
	
	/**
	 * MongoDB客户端
	 */
	private static MongoClient mongoClient;
	
	/**
	 * MongoDB客户端连接参数
	 */
	private static MongoClientOptions mongoClientOptions;
	
	/**
	 * 批量文档操作表
	 */
	ArrayList<Document> batchList=new ArrayList<Document>();
	
	/**
	 * MongoDB集合对象
	 */
	private static MongoCollection<Document> mongoCollection;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 主机列表
	 */
	private static ArrayList<ServerAddress> hostList=new ArrayList<ServerAddress>();
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * Sink默认过滤器
	 */
	private static final String DEFAULT_FILTER="com.bfw.flume.plugin.mdb.filter.impl.DefaultSinkFilter";
	
	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public synchronized void start() {
		super.start();
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public synchronized void stop() {
		if(null!=mongoClient) mongoClient.close();
		super.stop();
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status=Status.READY;
		Channel channel=getChannel();
		Transaction tx=channel.getTransaction();
		
		tx.begin();
		try{
			for(int i=0;i<batchSize;i++){
				Event event=channel.take();
				if(null==event){
					status=Status.BACKOFF;
					break;
				}
				
				String record=new String(event.getBody(),Charset.defaultCharset()).trim();
				if(0==record.length()) continue;
				
				Map<String,Object> doc=(Map<String,Object>)doFilter.invoke(filterObject, record);
				if(null==doc || 0==doc.size()) continue;
				
				String docIdVal=null;
				if(null!=docId && 0!=docId.length() && 0!=(docIdVal=doc.getOrDefault(docId, "").toString().trim()).length()) doc.put("_id", docIdVal);
				batchList.add(new Document(doc));
			}
			
			if(0!=batchList.size()){
				mongoCollection.insertMany(batchList);
				batchList.clear();
			}
			
			tx.commit();
			return status;
		}catch(Throwable e){
			e.printStackTrace();
			tx.rollback();
			return Status.BACKOFF;
		}finally{
			tx.close();
		}
	}
	
	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.conf.BatchSizeSupported#getBatchSize()
	 */
	@Override
	public long getBatchSize() {
		return batchSize;
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		//获取上下文参数1:过滤器名称
		filterName=getParamValue(context,"filterName", "filter");
		
		//获取上下文参数2:批处理尺寸
		batchSize=Integer.parseInt(getParamValue(context,"batchSize", "100"));
		
		//获取上下文参数3:MDB主机地址
		initHostAddress(context);
		
		//获取上下文参数4:MDB主机连接参数
		initMongoClientOptions(context);
		
		//装载自定义过滤器类路径
		try {
			addFilterClassPath();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		//装载过滤器配置
		Properties filterProperties=new Properties();
		try{
			System.out.println("INFO:====load filter config file:"+filterName+".properties");
			InputStream inStream=ClassLoaderUtil.getClassPathFileStream(filterName+".properties");
			filterProperties.load(inStream);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		//获取绑定的过滤器类
		Class<?> filterType=null;
		try {
			String filterClass=(String)filterProperties.remove("type");
			if(null==filterClass || 0==filterClass.trim().length()){
				filterClass=DEFAULT_FILTER;
				System.out.println("WARN:filterName=="+filterName+" the filter is empty or not found, the default filter will be used...");
			}
			System.out.println("INFO:====load filter class file:"+filterClass);
			filterType=Class.forName(filterClass);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		try {
			filterObject=filterType.newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			throw new RuntimeException("Error:filter object instance failure!!!");
		}
		
		//回调初始化插件参数
		try {
			Method pluginConfig = filterType.getDeclaredMethod("pluginConfig",Map.class);
			if(null!=pluginConfig) pluginConfig.invoke(filterObject, context.getParameters());
		} catch (Exception e) {
			System.out.println("Warn: "+filterType.getName()+" may not be initialized:contextConfig");
		}
		
		//自动初始化过滤器参数
		try {
			initFilter(filterType,filterProperties);
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("Warn: "+filterType.getName()+" may not be auto initialized:filterConfig");
		}
		
		//回调初始化过滤器参数
		try {
			Method filterConfig = filterType.getDeclaredMethod("filterConfig",Properties.class);
			if(null!=filterConfig) filterConfig.invoke(filterObject, filterProperties);
		} catch (Exception e) {
			System.out.println("Warn: "+filterType.getName()+" may not be manual initialized:filterConfig");
		}
		
		//初始化过滤器对象与接口表
		initFilterFace(filterType);
	}
	
	/**
	 * 初始化主机地址列表
	 * @param context 插件配置上下文
	 */
	private static final void initHostAddress(Context context){
		String[] hosts=COMMA_REGEX.split(getParamValue(context,"hostList", "127.0.0.1:27017"));
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(ServerAddressHelper.createServerAddress(ip, Integer.parseInt(port)));
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress("127.0.0.1", Integer.parseInt(unknow)));
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress(unknow, 27017));
			}
		}
	}
	
	/**
	 * 初始化MongoDB连接参数
	 * @param context 插件配置上下文
	 */
	private static final void initMongoClientOptions(Context context){
		Builder options = MongoClientOptions.builder();
		options.socketTimeout(Integer.parseInt(getParamValue(context,"socketTimeout", "0")));
		options.maxWaitTime(Integer.parseInt(getParamValue(context,"maxWaitTime", "5000")));
		options.connectTimeout(Integer.parseInt(getParamValue(context,"connectTimeout", "30000")));
		options.connectionsPerHost(Integer.parseInt(getParamValue(context,"connectionsPerHost", "300")));
		options.cursorFinalizerEnabled(Boolean.parseBoolean(getParamValue(context,"cursorFinalizerEnabled", "true")));
		
		WriteConcern writeConcern=WriteConcern.UNACKNOWLEDGED;
		
		try{
			String w=context.getString("w");
			w=null==w?null:w.trim();
			w=null==w||0==w.length()?null:w;
			if(null!=w) writeConcern=writeConcern.withW(w);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try{
			String wTimeoutMS=context.getString("wTimeoutMS");
			wTimeoutMS=null==wTimeoutMS?null:wTimeoutMS.trim();
			wTimeoutMS=null==wTimeoutMS||0==wTimeoutMS.length()?null:wTimeoutMS;
			if(null!=wTimeoutMS)writeConcern=writeConcern.withWTimeout(Long.parseLong(wTimeoutMS), TimeUnit.MILLISECONDS);
		}catch(Exception e){
			e.printStackTrace();
		}

		try{
			String journal=context.getString("journal");
			journal=null==journal?null:journal.trim();
			journal=null==journal||0==journal.length()?null:journal;
			if(null!=journal)writeConcern=writeConcern.withJournal(Boolean.parseBoolean(journal));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		options.writeConcern(writeConcern);
		mongoClientOptions=options.build();
	}
	
	/**
	 * 初始化过滤器接口
	 * @param dataBaseName 数据库名称
	 * @param collectionName 集合名称
	 * @param map 文档字典对象
	 */
	private static final void initFilterFace(Class<?> filterType) {
		String dataBaseName=null;
		String collectionName=null;
		try{
			String dataBaseNameStr=(String)filterType.getDeclaredMethod("getDataBaseName").invoke(filterObject);
			if(null==dataBaseNameStr || 0==(dataBaseName=dataBaseNameStr.trim()).length()) throw new RuntimeException("database name can not be NULL!!!");
			
			String collectionNameStr=(String)filterType.getDeclaredMethod("getCollectionName") .invoke(filterObject);
			if(null==collectionNameStr || 0==(collectionName=collectionNameStr.trim()).length()) throw new RuntimeException("collection name can not be NULL!!!");
			
			doFilter=filterType.getDeclaredMethod("doFilter",String.class);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		
		String userName=null;
		String passWord=null;
		try{
			userName=(String)filterType.getDeclaredMethod("getUsername").invoke(filterObject);
			passWord=(String)filterType.getDeclaredMethod("getPassword").invoke(filterObject);
		}catch(Exception e){
			System.out.println("Warn:===authentication information not found, will login anonymously...");
		}
		
		if(null==userName || null==passWord) {
			mongoClient=new MongoClient(hostList,mongoClientOptions);
		}else{
			mongoClient=new MongoClient(hostList,MongoCredential.createCredential(userName, dataBaseName, passWord.toCharArray()),mongoClientOptions);
		}
		
		MongoDatabase database=mongoClient.getDatabase(dataBaseName);
		mongoCollection=database.getCollection(collectionName,Document.class);
		if(null==mongoCollection) throw new RuntimeException("collection object can not be NULL!!!");
		
		try{
			String docIdStr=(String)filterType.getDeclaredMethod("getDocId").invoke(filterObject);
			docId=null==docIdStr?null:docIdStr.trim();
		}catch(Exception e){
			System.out.println("Warn:===no docid value was obtained, the default generated value will be used...");
		}
	}
	
	/**
	 * 初始化过滤器参数
	 * @param filterType 过滤类型
	 * @param filterProperties 过滤器参数字典
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static final void initFilter(Class<?> filterType,Properties filterProperties) throws IOException, ClassNotFoundException {
		if(null==filterType || 0==filterProperties.size()) return;
		for(Map.Entry<Object, Object> entry:filterProperties.entrySet()){
			String key=((String)entry.getKey()).trim();
			if(0==key.length()) continue;
			Field field=null;
			try {
				field=filterType.getDeclaredField(key);
				field.setAccessible(true);
			} catch (NoSuchFieldException | SecurityException e) {
				e.printStackTrace();
			}
			
			if(null==field) continue;
			
			Object value=null;
			try{
				value=TypeUtil.toType((String)entry.getValue(), field.getType());
			}catch(RuntimeException e){
				e.printStackTrace();
			}
			
			if(null==value) continue;
			
			try {
				if((field.getModifiers() & 0x00000008) == 0){
					field.set(filterObject, value);
				}else{
					field.set(filterType, value);
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 添加过滤器类路径
	 * @throws URISyntaxException
	 * @throws IOException 
	 */
	private static final void addFilterClassPath() throws URISyntaxException, IOException{
		File file = new File(new File(MongoSink.class.getResource("/").toURI()).getParentFile(),"filter");
		if(!file.exists()) file.mkdirs();
		ClassLoaderUtil.addFileToCurrentClassPath(file, MongoSink.class);
	}
	
	/**
	 * 获取参数值
	 * @param context Sink插件上下文
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final String getParamValue(Context context,String key,String defaultValue){
		String value=context.getString(key,defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
	
}
