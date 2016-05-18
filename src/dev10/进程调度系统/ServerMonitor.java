public  class ServerMonitor implements Watcher,Runnable{
private ZooKeeper  zooKeeper;
private String connectString;
private int session Timeout;
private String hadoopHome;
private String mapredJob Trachker;
//��ʼ���ļ����أ���������������ZooKeeper������������
public void initConf() throws  Exeception{
InitConfReader reader=new InitConfReader(��init.properties��);
List<String> keys=new ArrayList<String>();
keys.add(��connectString��);
keys.add(��sessionTimeout��);
keys.add(��hadoopHome��);
keys.add(��mapred.job.tracher��);
Map<String , String>confs=reader.getConfs(keys);
this.hadoopHome=confs.get(��hadoopHome��);
this.mapredJobTracker=confs.get(��mapred.job.tracker��);
zooKeeper=new ZooKeeper(connectString,sessionTimeout,this);
//���ӽڵ��д洢������״̬�仯��
public ServerMonitor() throws Exeeption{
SchedulingServer schedulingServer=new SchedulingServer();
schedulingServer.initConf();
schedulingServer.initServer();
initConf();
}
public void process(WatchedEvent   event){
}
/*
һ��������ܳ��ڣ��ȴ������У��ɹ���ʧ�ܣ�ɱ����״̬�е�һ��
1.������ڵȴ�������״̬�������κβ����������������״̬��֪��״̬�����仯
2.������ڳɹ�״̬���ӡ�/root/client/wait����ɾ������������뵽��/root/clients/processed�����У���ֹͣ�Ըýڵ���м��
3.�����һ�γ���ʧ�ܻ�ɱ��״̬����������롰/root/client/temp���У����ص�������������ζ���ʧ�ܻ�ɱ����������롱/root/client/error����ֹͣ�Դ�����ļ�⡣
*/
public void monitorNode() throws  Exception{
List<String>waits=zooKeeper.getChildren(��/root/clien/wait�� ,false);
if(!waits.isEmpty()){
         JobConf conf=new JobConf();
         conf.set(��mapred.job.tracker�� , mapredJobTracker);
         JobClient  jobClient=new  JobClient(conf);
for(String wait:waits){
          String data=new String(zooKeeper.getdata(��/root/client/wait/��+wait,false,null);
        JobID jobid=null;
try{
       jobid=JobID.forName(wait);
}catch(Excption e){
       System.out.println(��hob id  is wrong!!!��) ;
Stat stat=zooKeeper.exists(��/root/client/error/��+wait,false);
if(stat!=null){
        zooKeeper.delete(��/root/client/error/��+wait,-1);
    }
zooKeeper.delete(��/root/client/wait/��+wait,-1);
zooKeeper.create(��/root/client/error/��+wait,data,getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
 continus;
}
//ͨ�������JOBID������������ڴ��ڵ�״̬
int runStat=jobClient.getJob(org.apache.hadoop.mapred.JobID)jobid).getJobState();
switch(runStat){
//���ڵȴ�������״̬��������״̬�������ı�ǰ��������
case JobStatus.RUNNING:
case JobStatus.PREP:
braek;
//������ִ�гɹ���ɾ��ԭ��/root/wait��Ŀ¼�µĽڵ㲢����������Ϣ���뵽��/root/wait/processed��
case JobStatus.SUCCEEDED:
        zooKeeper.delete(��/root/client/wait/��+wait,-1);
        zooKeeper.create(��/root/client/processed/��+wait,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
   List<String>tempNodes=zooKeeper.getChildren(��/root/client/temp��,false);
   if(tempNodes==null || tempNodes.size()==0) {
break;
}
else {
    for(String tempNode:tempNodes){
         if(new String(zooKeeper,getData(��/root/client/temp��+tempNode,false,null)).equals(data)){
              zooKeeper.delete(��/root/client/temp/��+tempNode,-1);
}
}
}
break;
//������ִ��ʧ�ܻ���������ɱ���������������롰/root/temp���²��ص������������ص���ʧ�ܣ���������롱root/error��
case  JobSatus.FAILED:
case JobStatus.KILLED:
        zooKeeper.delete(��/root/client/wait/��+wait,-1);
       tempNodes=zooKeeper.getChildren(��/root/client/temp��,false);
       zooKeeper.create(��/root/client/temp��+wait,data.getBytes(),Ids.OPEN_ACL_UNSAFE,
CreateMode.PERSISTENT);
if(tempNodes==null  ||  tempNode.size()==0){
//����shell����ص�
shellTool.callBack(data,hadoopHome);
}else{
boolean flag=true;
for(String tempNode:tempNodes){
if(new String(zooKeeper.geData(��/root/client/temp��+temNode,false,null)).equals(data)){
     zooKeeper.create(��/root/client/error/��+wait,data.getBytes(),
Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
zooKeeper.delete(��/root/client/temp/��+wait, -1);
zooKeeper.delete(��/root/client/temp��+ tempNode, -1);
flag=false;
}
}
if(flag){
//����shell����Ļص�
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

