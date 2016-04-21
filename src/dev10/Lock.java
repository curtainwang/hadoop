package org.jiacheo.zkdl.lock;  
import <a title="See also Java
如何等待子线程执行结束" href="http://www.jiacheo.org/blog/262">java</a>.net.InetAddress;  
import <a title="See also tomcat thread dump 
分析" href="http://www.jiacheo.org/blog/279">java</a>.net.UnknownHostException;  
import org.apache.zookeeper.KeeperException;  
import org.apache.zookeeper.ZooKeeper;  
import org.apache.zookeeper.data.Stat;  
/** 
 * 类名：<b>Lock</b> <br/> 
 * <p> 
 * 类描述：  
 * </p> 
 * 创建人：jiacheo <br/> 
 * 创建时间：2011-1-27 上午01:30:25  <br/>   
 * @version 2011-1-27   
 * 
 */  
public class Lock {  
    private String path;  
    private ZooKeeper zooKeeper;  
    public Lock(String path){  
        this.path = path;  
    }  
       
    /** 
     * <p> 
     * 方法描述: 上锁 lock it 
     * </p> 
     * 创建人：jiacheo <br/> 
     * 创建时间：2011-1-27 上午01:30:50  <br/> 
     * @throws Exception 
     */  
    public synchronized void lock() throws Exception{  
        Stat stat = zooKeeper.exists(path, true);  
        String data = InetAddress.getLocalHost().getHostAddress()+":lock";  
        zooKeeper.setData(path, data.getBytes(), stat.getVersion());  
    }  
       
    /** 
     * <p> 
     * 方法描述：开锁 unlock it 
     * </p> 
     * 创建人：jiacheo <br/> 
     * 创建时间：2011-1-27 上午01:31:20  <br/> 
     * @throws Exception 
     */  
    public synchronized void unLock() throws Exception{  
        Stat stat = zooKeeper.exists(path, true);  
        String data = InetAddress.getLocalHost().getHostAddress()+":unlock";  
        zooKeeper.setData(path, data.getBytes(), stat.getVersion());  
    }  
       
    /** 
     * <p> 
     * 方法描述：是否锁住了, isLocked? 
     * </p> 
     * 创建人：jiacheo <br/> 
     * 创建时间：2011-1-27 上午01:31:43  <br/> 
     * @return 
     */  
    public synchronized boolean isLock(){  
        try {  
            Stat stat = zooKeeper.exists(path, true);  
            String data = InetAddress.getLocalHost().getHostAddress()+":lock";  
            String nodeData = new String(zooKeeper.getData(path, true, stat));  
            if(data.equals(nodeData)){  
//              lock = true;  
                return true;  
            }  
        } catch (UnknownHostException e) {  
            // ignore it  
        } catch (KeeperException e) {  
            //TODO use log system and throw a new exception  
        } catch (InterruptedException e) {  
            // TODO use log system and throw a new exception  
        }  
        return false;  
    }  
   
    public String getPath() {  
        return path;  
    }  
   
    public void setPath(String path) {  
        this.path = path;  
    }  
   
    public void setZooKeeper(ZooKeeper zooKeeper) {  
        this.zooKeeper = zooKeeper;  
    }  
 
}  

//- - - -LockFactory.java- - - - //
package org.jiacheo.zkdl.lock;  
   
import java.io.IOException;  
import java.net.InetAddress;  
import java.util.Collections;  
   
import org.apache.zookeeper.CreateMode;  
import org.apache.zookeeper.WatchedEvent;  
import org.apache.zookeeper.Watcher;  
import org.apache.zookeeper.ZooKeeper;  
import org.apache.zookeeper.ZooDefs.Ids;  
import org.apache.zookeeper.ZooDefs.Perms;  
import org.apache.zookeeper.data.ACL;  
import org.apache.zookeeper.data.Stat;  
   
public class LockFactory {  
       
    public static final ZooKeeper DEFAULT_ZOOKEEPER = getDefaultZookeeper();  
    //data格式:  ip:stat  如: 10.232.35.70:lock 10.232.35.70:unlock  
    public static synchronized Lock getLock(String path,String ip) throws Exception{  
        if(DEFAULT_ZOOKEEPER != null){  
            Stat stat = null;  
            try{  
                stat = DEFAULT_ZOOKEEPER.exists(path, true);  
            }catch (Exception e) {  
                // TODO: use log system and throw new exception  
            }  
            if(stat!=null){  
                byte[] data = DEFAULT_ZOOKEEPER.getData(path, null, stat);  
                String dataStr = new String(data);  
                String[] ipv = dataStr.split(":");  
                if(ip.equals(ipv[0])){  
                    Lock lock = new Lock(path);  
                    lock.setZooKeeper(DEFAULT_ZOOKEEPER);  
                    return lock;  
                }  
                //is not your lock, return null  
                else{  
                    return null;  
                }  
            }  
            //no lock created yet, you can get it  
            else{  
                createZnode(path);  
                Lock lock = new Lock(path);  
                lock.setZooKeeper(DEFAULT_ZOOKEEPER);  
                return lock;  
            }  
        }  
        return null;  
    }  
       
    private static ZooKeeper getDefaultZookeeper() {  
        try {  
            ZooKeeper zooKeeper = new ZooKeeper("10.232.35.72", 10*1000, new Watcher(){  
                public void process(WatchedEvent event) {  
                    //节点的事件处理. you can do something when the node's data change  
//                  System.out.println("event " + event.getType() + " has happened!");  
                }  
            });  
            return zooKeeper;  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return null;  
    }  
    private static void createZnode(String path) throws Exception{  
 if(DEFAULT_ZOOKEEPER!=null){  
            InetAddress address = InetAddress.getLocalHost();  
            String data = address.getHostAddress()+":unlock";  
            DEFAULT_ZOOKEEPER.create(path, data.getBytes(),Collections.singletonList(new ACL(Perms.ALL,Ids.ANYONE_ID_UNSAFE)) , CreateMode.EPHEMERAL);  
        }  
    }  
}  
