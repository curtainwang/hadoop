public  class ServerMonitor implements Watcher,Runnable{
private ZooKeeper  zooKeeper;
private String connectString;
private int session Timeout;
private String hadoopHome;
private String mapredJob Trachker;
//初始化文件加载，并用其内容配置ZooKeeper服务器的连接
public void initConf() throws  Exeception{
InitConfReader reader=new InitConfReader(“init.properties”);
List<String> keys=new ArrayList<String>();
keys.add(“connectString”);
keys.add(“sessionTimeout”);
keys.add(“hadoopHome”);
keys.add(“mapred.job.tracher”);
Map<String , String>confs=reader.getConfs(keys);
this.hadoopHome=confs.get(“hadoopHome”);
this.mapredJobTracker=confs.get(“mapred.job.tracker”);
zooKeeper=new ZooKeeper(connectString,sessionTimeout,this);
//监视节点中存储的任务状态变化的
public ServerMonitor() throws Exeeption{
SchedulingServer schedulingServer=new SchedulingServer();
schedulingServer.initConf();
schedulingServer.initServer();
initConf();
}
public void process(WatchedEvent   event){
}
/*
一个任务可能出于：等待，运行，成功，失败，杀死等状态中的一个
1.任务出于等待或运行状态，不做任何操作，继续检测任务状态，知道状态发生变化
2.任务出于成功状态，从”/root/client/wait”中删除，并将其插入到”/root/clients/processed”当中，并停止对该节点进行检测
3.程序第一次出于失败或杀死状态，将任务插入“/root/client/temp”中，并回调，如果连续两次都是失败或被杀死，则将其插入”/root/client/error”并停止对此任务的检测。
*/
public void monitorNode() throws  Exception{
List<String>waits=zooKeeper.getChildren(“/root/clien/wait” ,false);
if(!waits.isEmpty()){
         JobConf conf=new JobConf();
         conf.set(“mapred.job.tracker” , mapredJobTracker);
         JobClient  jobClient=new  JobClient(conf);
for(String wait:waits){
          String data=new String(zooKeeper.getdata(“/root/client/wait/”+wait,false,null);
        JobID jobid=null;
try{
       jobid=JobID.forName(wait);
}catch(Excption e){
       System.out.println(“hob id  is wrong!!!”) ;
Stat stat=zooKeeper.exists(“/root/client/error/”+wait,false);
if(stat!=null){
        zooKeeper.delete(“/root/client/error/”+wait,-1);
    }
zooKeeper.delete(“/root/client/wait/”+wait,-1);
zooKeeper.create(“/root/client/error/”+wait,data,getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
 continus;
}
//通过任务的JOBID来检测任务正在处于的状态
int runStat=jobClient.getJob(org.apache.hadoop.mapred.JobID)jobid).getJobState();
switch(runStat){
//处于等待和运行状态的任务在状态不发生改变前不做处理
case JobStatus.RUNNING:
case JobStatus.PREP:
braek;
//当任务执行成功后，删除原”/root/wait”目录下的节点并将其任务信息插入到“/root/wait/processed”
case JobStatus.SUCCEEDED:
        zooKeeper.delete(“/root/client/wait/”+wait,-1);
        zooKeeper.create(“/root/client/processed/”+wait,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
   List<String>tempNodes=zooKeeper.getChildren(“/root/client/temp”,false);
   if(tempNodes==null || tempNodes.size()==0) {
break;
}
else {
    for(String tempNode:tempNodes){
         if(new String(zooKeeper,getData(“/root/client/temp”+tempNode,false,null)).equals(data)){
              zooKeeper.delete(“/root/client/temp/”+tempNode,-1);
}
}
}
break;
//当任务执行失败或者任务呗杀掉，将会把任务插入“/root/temp”下并回调任务，如果任务回调后失败，则将任务插入”root/error”
case  JobSatus.FAILED:
case JobStatus.KILLED:
        zooKeeper.delete(“/root/client/wait/”+wait,-1);
       tempNodes=zooKeeper.getChildren(“/root/client/temp”,false);
       zooKeeper.create(“/root/client/temp”+wait,data.getBytes(),Ids.OPEN_ACL_UNSAFE,
CreateMode.PERSISTENT);
if(tempNodes==null  ||  tempNode.size()==0){
//用于shell命令回调
shellTool.callBack(data,hadoopHome);
}else{
boolean flag=true;
for(String tempNode:tempNodes){
if(new String(zooKeeper.geData(“/root/client/temp”+temNode,false,null)).equals(data)){
     zooKeeper.create(“/root/client/error/”+wait,data.getBytes(),
Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
zooKeeper.delete(“/root/client/temp/”+wait, -1);
zooKeeper.delete(“/root/client/temp”+ tempNode, -1);
flag=false;
}
}
if(flag){
//用于shell命令的回调
   ShellTool.callBack(data, hadoopHome);
}
}
break;
default:
    break;
}
}
}
}
public  void  run()  {
  try  {
   ServerMonitor serverWaitMonitor = new  ServerMonitor();
   while(true){
  ServerWaitMonitor.monitorNode();
Thread.sleep(5000);
}
}
catch(Exception e){
   e.printStackTrace();
}
}
public static void  main(String[] args) throws Exception{
    Thread thread =new  Thread(new  ServerMonitor());
thread.start();
}
}

