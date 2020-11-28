### 插件开发背景
MongoSink是Flume流处理工具下基于MongoDB的一款Sink插件，截止目前的Flume1.9版本发布包，还不曾携带基于MongoDB数据库的Sink插件，为实现Flume流处理工具能够很好的对接到MongoDB数据库服务，特开发MongoSink插件，该插件基于MongoDB驱动3.12.7版本开发，即：mongodb-driver-3.12.7


​      
### 插件功能特性
1. 版本无关性  
MongoSink插件被设计成不依赖于任何MongoDB版本（即它与MongoDB版本无关），因为他是基于mongodb协议实现（mongodb协议是建立在TCP协议层之上的应用层协议），除了自身issue需要复验以外，不会因为任何版本问题导致其插件启动失败或是MongoDB服务连接失效

2. 插件扩展性  
这是一款Flume-Sink插件，它除了基于默认配置来完成一些简单的基础过滤功能，还提供了基于JAVA语言自定义的过滤器扩展，使用者可以根据自己的业务定制编写自己的个性化过滤器并将其放置到Flume安装目录下的filter目录中，同时配置好使用自定义过滤器，该插件即可回调自定义过滤器完成日志记录的过滤操作    
   
   ​     

### 插件使用说明
#### Flume工具及插件安装
1. 下载JDK-1.8.271  
wget https://download.oracle.com/otn/java/jdk/8u271-b09/61ae65e088624f5aaa0b1d2d801acb16/jdk-8u271-linux-x64.tar.gz
  
    
   
2. 安装JDK-1.8.271  
tar -zxvf jdk-8u271-linux-x64.tar.gz -C /software/jdk1.8.0_271  
echo -e "JAVA_HOME=/software/jdk1.8.0_271\nPATH=$PATH:$JAVA_HOME/lib:$JAVA_HOME/bin\nexport PATH JAVA_HOME">>/etc/profile && source /etc/profile
  
    
   
3. 下载Flume-1.9.0  
wget https://github.com/lixiang2114/Software/raw/main/flume-1.9.0.zip
  
    
   
4. 安装Flume-1.9.0  
unzip flume-1.9.0.zip -d /software/
  
    
   
5. 下载插件MongoSink-1.0  
wget https://github.com/lixiang2114/MongoSink/raw/main/depends.zip
  
    
   
6. 安装插件MongoSink-1.0  
unzip depends.zip   &&   cp -a depends/*   /software/flume-1.9.0/lib/  
   
    

​      
#### MongoDB服务安装
1. 下载MongoDB  
wget https://github.com/lixiang2114/Software/raw/main/mongodb-4.2.6.zip  
  
    
   
2. 安装MongoDB  
unzip mongodb-4.4.1.zip -d /software/      

 

说明：    
若搭建MDB副本集群或分片集群，请参MongoDB官网给出的文档，推荐构建分片集群，因为MongoDB通常处理的数据量都较大    


​      
#### MongoSink插件基础使用
**  Note：**下面以抽取日志为例来说明插件的基本使用方法    

1. 编写Shell命令或脚本  
```Shell
vi /software/flume-1.9.0/process/script/getLogger.sh
#!/usr/bin/env bash
while true;do
    tailf -0 /install/test/mylogger.log 2>/dev/null
    sleep 1s
done

chmod a+x /software/flume-1.9.0/process/script/getLogger.sh
```


2. 编写Flume任务流程配置  
```Text
vi /software/flume-1.9.0/process/conf/example01.conf
a1.sources=s1
a1.sinks=k1 k2
a1.channels=c1 c2

a1.sources.s1.type=exec
a1.sources.s1.command=/software/flume-1.9.0/process/script/getLogger.sh
a1.sources.s1.batchSize=20
a1.sources.s1.batchTimeout=3000
a1.sources.s1.restart=true
a1.sources.s1.restartThrottle=10000
a1.sources.s1.channels=c1 c2
a1.sources.s1.selector.type=replicating

a1.sinks.k1.type=logger
a1.sinks.k1.channel=c1

a1.sinks.k2.type=com.github.lixiang2114.flume.plugin.mdb.MongoSink
a1.sinks.k2.hostList=192.168.162.130:27017
a1.sinks.k2.fieldList=times,level,message
a1.sinks.k2.filterName=mdbFilter
a1.sinks.k2.fieldSeparator=,
a1.sinks.k2.dataBaseName=mdbtest
a1.sinks.k2.collectionName=logger
a1.sinks.k2.docId=times
a1.sinks.k2.channel=c2

a1.channels.c1.type=memory
a1.channels.c1.capacity=1000
a1.channels.c1.transactionCapacity=100
a1.channels.c2.type=memory

a1.channels.c2.capacity=1000
a1.channels.c2.transactionCapacity=100
```


3. 启动MongoDB服务  
```Shell
su -l elastic
/software/mongodb-4.4.1/sbin/SingleTools start
lsof -i tcp:27017
```


4. 启动Flume服务  
```Shell
/software/flume-1.9.0/bin/flume-ng agent -c /software/flume-1.9.0/conf -f /software/flume-1.9.0/process/conf/example01.conf -n a1 -Dflume.root.logger=INFO,console
```


5. 使用Shell模拟日志产生以测试Flume插件  
```Shell
for index in {1..100000};do echo "${index},info,this is my ${index} times test";echo "${index},info,this is my ${index} times test">> /install/test/mylogger.log;sleep 0.001s;done
```


​      
​      

#### MongoSink插件过滤器使用  
##### 过滤器接口规范简介
不同的Sink组件可以对应到不同的插件过滤器，编写插件过滤器的接口规范如下：  
```JAVA
package com.github.lixiang2114.flume.plugin.mdb.filter;

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
	public Map<String,Object> doFilter(String record);
	
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
	 * 插件上下文配置(可选实现)
	 * @param config 配置
	 */
	default public void pluginConfig(Map<String,String> config){}
}
```
说明：  
编写插件过滤器通常需要实现SinkFilter接口，但这并不是必须的，考虑到程序员编码的灵活性，MongoSink插件被设计成约定优于配置的原则，因此程序员只需要在自定义的过滤器实现类中提供相应的接口规范即可，MongoSink总是可以根据接口规范检索到对应的接口签名并正确无误的去回调它   


​    
##### 自定义过滤器实现步骤  
1. 编写过滤器实现类  
```JAVA
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.github.lixiang2114.flume.plugin.mdb.filter.MdbSinkFilter;

/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class MdbLoggerFilter implements MdbSinkFilter{
	/**
	 * 字段列表
	 */
	private String[] fields;
	
	/**
	 * 文档索引类型
	 */
	private String collectionName;
	
	/**
	 * 文档索引名称
	 */
	private String dataBaseName;
	
	/**
	 * 日志记录字段分隔符
	 */
	private String fieldSeparator;
	
	/**
	 * 逗号正则式
	 */
	private static Pattern commaRegex;
	
	@Override
	public String getDocId() {
		return fields[0]; 
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
	public Map<String, Object> doFilter(String record) { 
		String[] fieldValues=commaRegex.split(record);
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put(fields[0], fieldValues[0].trim());
		map.put(fields[1], fieldValues[1].trim());
		map.put(fields[2], fieldValues[2].trim());
		return map;
	}

	@Override
	public void filterConfig(Properties properties) {
		commaRegex=Pattern.compile(fieldSeparator);
	}
}
```

说明：  
上面实现的接口MdbSinkFilter来自于FlumePluginFilter.jar包，我们可以从github上下载获得：
wget https://github.com/lixiang2114/Document/raw/main/plugin/flume1.9/face/FlumePluginFilter.jar  
可以使用Eclipse、Idea等IDE集成开发工具来完成上述编码和编译过程，如果过滤器项目是基于Maven构建的，还可以直接使用Maven来编译项目，如果过滤器简单到只有单个类文件也可以直接使用命令行编译：  
javac -cp FlumePluginFilter.jar MdbLoggerFilter.java  

如果编译后的项目不止一个字节码文件则需要打包：  
Maven： mvn package -f  /xxx/pom.xml  
JAVA：jar -cvf xxx.jar -C \[project\]  


​    
2. 发布过滤器  
* 发布过滤器代码  
不论过滤器项目编译后是单个字节码文件还是压缩打成的jar包，我们都可以直接将其拷贝到filter目录下的lib子目录中即可：  
cp -a MdbLoggerFilter.class /software/flume-1.9.0/filter/lib/  
或  
cp -a MdbLoggerFilter.jar /software/flume-1.9.0/filter/lib/  
  
    
  
* 配置发布的过滤器  
```Text
vi /software/flume-1.9.0/filter/mdbFilter.properties  
type=MdbLoggerFilter
collectionName=logger
dataBaseName=mdbtest
fieldSeparator=,
fields=docId,level,msg
```


说明：  
因为上述的MdbLoggerFilter非常简单，就是一个字节码文件，没有定义包名（即存在于类路径下的默认包中），所以看到的就是一个类名，如果过滤器的入口类（实现SinkFilter接口的类）有包名则必须带上包名  

经过以上步骤之后，我们启动Flume服务，MongoSink插件就会自动调动我们自定义的过滤器类MdbLoggerFilter来完成日志过滤处理了  



​    
##### 过滤器高级应用  
MongoSink插件支持多实例Sink复用，即不同的Sink实例可以重用MongoSink插件，假如我们有两个MongoDB的集群构建，我们希望于按业务线或模块将日志过滤成不同的输出并推送到对应的两个不同MongoDB集群服务上，那么我们可以在Flume的任务流程配置中配置好两个不同的Sink实例，这两个Sink实例中的数据分别来自于不同的通道Channel，同时为两个不同的Sink实例指定不同的过滤器参数名（使用参数名filterName指定，默认提供的filterName参数值是filter）：    
      
```Text
a1.sinks.k1.type=com.github.lixiang2114.flume.plugin.mdb.MongoSink
a1.sinks.k1.hostList=192.168.162.129:27017,192.168.162.130:27017,192.168.162.131:27017
a1.sinks.k1.filterName=filter01
a1.sinks.k1.channel=c1  

a1.sinks.k2.type=com.github.lixiang2114.flume.plugin.mdb.MongoSink
a1.sinks.k2.hostList=192.168.162.132:27017,192.168.162.133:27017,192.168.162.134:27017
a1.sinks.k2.filterName=filter02
a1.sinks.k2.channel=c2    
```

然后在filter目录下指定对应的过滤器配置文件即可（根据约定优于配置的原则，我们定义的文件名需要与filterName参数值保持相同，比如默认文件名为：filter.properties），一个典型的过滤器配置形如下面给出的格式：    
    
```Text
cat filter01.properties
type=UserInfoFilter
collectionName=userInfo
dataBaseName=user
fieldSeparator=,
fields=userId,userName,group,balance    
    
        
cat filter02.properties
type=OrderInfoFilter
collectionName=orderInfo
dataBaseName=order
fieldSeparator=,
fields=orderId,orderName,price,userId      
```

最后还需要分别编写过滤器类UserInfoFilter和OrderInfoFilter，注意上面定义的这两个类都没有包名，这说明它们被放在默认的classpath的类路径根目录下，为了便于简化程序员的编码和部署工作，MongoSink插件允许对一些非常简单的过滤操作只需要编写一个单类即可，编译好这个单类并将它拷贝到filter目录下即完成快捷部署。当然如果对于一些过滤非常复杂的操作（比如在过滤中涉及到一些业务逻辑的处理等），我们也可以启动一个完整的JAVA工程或Maven工程来编写过滤器，最后将其打包成jar文件拷贝到filter目录下，** 过滤器的编写参见上述章节的讲解 **    
    
程序员在自定义过滤器实现的过程中，其过滤器类中成员变量名应该与过滤器配置文件中的参数名保持一致，这将有利于MongoSink插件自动化初始化类的成员，同时在过滤器规范中有有以下两个接口是可选的实现：    

```JAVA
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
* 插件上下文配置(可选实现)
* @param config 配置
*/
default public void pluginConfig(Map<String,String> config){}
```

除非有特别的必要，否则程序员编写过滤器无需实现pluginConfig接口，该接口回调传入的字典参数来自于插件上下文配置（即flume进程启动时由-f参数显式指定的配置文件），而对于filterConfig接口的实现对于开发工程师而言也是可选的，为了尽量减轻开发工程师编码的复杂性，MongoSink插件会在初始化插件上下文参数后自动为开发工程师定义的过滤器类完成一次基于过滤器成员变量的依赖注入，以保证在插件在回调过滤器的doFilter方法之前已经充分准备好了所需的过滤器参数，当然开发工程师也可以手动重写此方法以覆盖插件的初始化结果
            
​    
**  备注： **    
MongoSink插件启动时会自动将Flume安装目录下的filter子目录递归装载到JVM的CLASSPATH路径下，因此在filter目录下的任何子目录都将存在于类路径的根目录下，所以，运维工程师或开发工程师可以随时将过滤器的配置文件、字节码文件或打包好的JAR文件等放入filter目录下的任何位置均可，MongoSink插件总是可以准确无误的找到并读取他们；这一点是非常重要的，它保证了放入此目录下的任何文件都将存在于CLASSPATH路径上，程序员自定义的过滤器可以毫无障碍的找到并实现过滤器的上下文参数配置；为了方便在配置和代码多了之后，其后期维护难度不至于过大，我们建议开发工程师和运维工程师应该在此目录下建立起更易于方便阅读的目录结构，然后再将过滤器的配置文件、过滤器字节码或过滤器打包JAR文件放置到相应的目录下，一个典型的目录结构设计形如下面的形式：    
      

```Shell
[root@CC7 filter]# pwd
/software/flume-1.9.0/filter
[root@CC7 filter]# tree
.
├── conf
│   └── LogFilter.properties
└── lib
    └── LoggerFilter.class

2 directories, 2 files
```


​    
##### MongoSink安全认证  
如果你的存储介质中保存的是与用户信息无关的脱敏数据，同时存储服务部署于内网，则不建议使用安全认证，因为安全认证本身将给内网通信带来更多的附加网络阻力，如果在特定的场景下需要MongoDB做安全认证，则可以在MongoDB服务中开启安全认证，这需要首先在MongoDB中配置数据库用户：  
```Shell
[root@CC9 sbin]# pwd
/software/mongodb-4.4.1/sbin
./SingleTools addAdmin -au root -ap 123456
./SingleTools addUser -d mdbtest -au ligang -ap 123456
```
addAdmin是为admin数据库增加一个超级用户root，第二个addUser是为指定的mdbtest数据库增加用户ligang，超级用户的角色为root，拥有所有读写和执行权限，普通用户ligang只有读写权限，其中-d参数执行增加用户所在的数据库，-au指定新增用户的登录用户名，-ap参数指定新增用户的登录密码，最后启动MongoDB服务时需要带上-a参数（--auth）表示任何形式的客户端登录到MongoDB数据库都需要验证：   
```Shell
[root@CC9 sbin]# pwd
/software/mongodb-4.4.1/sbin
[root@CC9 sbin]# ./SingleTools start -a
```
​    
MongoSink插件也支持MongoDB的安全认证，这需要通过配置和过滤器来实现，具体操作步骤详情如下：  
1. 在Flume插件配置文件或过滤器配置文件中增加登录认证信息  
* 在插件配置文件中增加  
```Text
a1.sinks.k2.userName=ligang
a1.sinks.k2.passWord=123456  
```
* 在过滤器配置文件中增加  
```Text
userName=ligang
passWord=123456  
```

2. 在自定义过滤器中覆盖以下方法并返回用户名和密码  
```
import com.github.lixiang2114.flume.plugin.mdb.filter.MdbSinkFilter;
/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class MdbLoggerFilter implements MdbSinkFilter{
	/**
	 * 登录Elastic用户名
	 */
	private static String userName;
	
	/**
	 * 登录Elastic密码
	 */
	private static String passWord;
	...............................
	...............................
	/**
	 * 获取登录密码
	 * @return 密码
	 */
	default public String getPassword(){
		return userName;
	}
	
	/**
	 * 获取登录用户名
	 * @return 用户名
	 */
	default public String getUsername(){
		return passWord;
	}
}
```