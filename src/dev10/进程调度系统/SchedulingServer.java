public class SchedulingServer implments Watcher{
private ZooKeeper zooKeeper;
privae String connectString; //conectString�����ַ���������IP��ַ���������˿ں�
private int  session Timeout;
public void initConf() thows Exception{
InitConfReader reader=new InitConfReader(��init.properties��);
List<String>keys=new ArrayList<String>();
keys.add(��connectSring��);
keys.add(��sessionTimeout��);
Map<String  ,String>confs=reader.getConfs(keys);
this.connectString=conf.get(��connectString��);
this.sessionTimeout=Integer.parseInt(confs.get(��sessionTimeout��));
zooKeeper=new ZooKeeper(connectString, sessionTimeout ,this);
}
/*
1.����ϵͳ�е����о�̬�ڵ����������
2.��/root���ڵ㱻��������/root/client��δ������
3.��/root/client���������������ӽڵ�һ������δ������.
4.�洢״̬��һ�������ڵ�δ������
*/
public void initServer() throws  Exception{
//stat���ڴ洢�����ڵ��Ƿ���ڣ������������Ӧ��ֵΪnull
Stat stat=zooKeeprr.exits(��/root�� , false);
if(stat==null){
//���ڵ�
zooKeeper.create(��/root��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//ʧ������洢�ڵ�
zooKeeper.create(��/root/error��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//�ɹ�����洢�ڵ�
zooKeeper.create(��/root/processed��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//�ȴ���������������洢�ڵ�
zooKeeper.create(��/root/wait��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
//��ʱ�洢��һ�δ���ʧ�ܵĽڵ�
zooKeeper.create(��/root/temp��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
stat=zooKeeper.exists(��root/error��, false);
if(stat==null){
zooKeeper.create(��/root/error��,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISITENT);
}
stat=zooKeeper.exists(��/root/processed��,false);
if(stat==null){
zooKeeper.create(��/root/processed��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
stat=zooKeeper.exists(��root/wait��,false);
if(stat==null){
zooKeeper.create(��/root/wait��,null,Ids.OPEN_ACL_UNSAFE,createMode.PERSISTENT);
}
}
public void process(WatchedEvent event){
}
}
