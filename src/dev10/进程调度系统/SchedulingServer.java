public class SchedulingServer implments Watcher{
private ZooKeeper zooKeeper;
privae String connectString; //conectString连接字符串，包括IP地址，服务器端口号
private int  session Timeout;
public void initConf() thows Exception{
InitConfReader reader=new InitConfReader(“init.properties”);
List<String>keys=new ArrayList<String>();
keys.add(“connectSring”);
keys.add(“sessionTimeout”);
Map<String  ,String>confs=reader.getConfs(keys);
this.connectString=conf.get(“connectString”);
this.sessionTimeout=Integer.parseInt(confs.get(“sessionTimeout”));
zooKeeper=new ZooKeeper(connectString, sessionTimeout ,this);
}
/*
1.整个系统中的所有静态节点均被创建。
2.“/root”节点被创建，“/root/client”未被创建
3.“/root/client”被创建但其下子节点一个或多个未被创建.
4.存储状态的一个或多个节点未被创建
*/
public void initServer() throws  Exception{
//stat用于存储被监测节点是否存在，若不存在则对应的值为null
Stat stat=zooKeeprr.exits(“/root” , false);
if(stat==null){
//根节点
zooKeeper.create(“/root”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//失败任务存储节点
zooKeeper.create(“/root/error”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//成功任务存储节点
zooKeeper.create(“/root/processed”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//等待和正在运行任务存储节点
zooKeeper.create(“/root/wait”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//临时存储第一次处理失败的节点
zooKeeper.create(“/root/temp”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
stat=zooKeeper.exists(“root/error”, false);
if(stat==null){
zooKeeper.create(“/root/error”,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISITENT);
}
stat=zooKeeper.exists(“/root/processed”,false);
if(stat==null){
zooKeeper.create(“/root/processed”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
stat=zooKeeper.exists(“root/wait”,false);
if(stat==null){
zooKeeper.create(“/root/wait”,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
}
public void process(WatchedEvent event){
}
}
