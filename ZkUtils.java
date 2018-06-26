package com.tangzhe.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Created by 唐哲
 * 2018-06-24 16:26
 * zk操作工具类
 */
public class ZkUtils {

    private static final Integer ZK_SESSION_TIMEOUT = 10 * 1000;
    private static CountDownLatch latch = new CountDownLatch(1);
    private static ZkUtils instance = new ZkUtils();
    private static ZooKeeper zk;

    public synchronized static ZkUtils getInstance(String host, int port) {
        if (zk == null) {
            connect(host, port);
        }
        return instance;
    }

    private static void connect(String host, int port) {
        String connectString = host + ":" + port;
        try {
            zk = new ZooKeeper(connectString, ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");
                    // 判断是否已连接ZK, 连接后计数器递减
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            // 若计数器不为0, 则等待
            latch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String addNode(String nodeName) {
        Stat stat;
        try {
            stat = zk.exists(nodeName, false);
            if (stat == null) {
                return zk.create(nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String addNode(String nodeName, String data) {
        Stat stat;
        try {
            stat = zk.exists(nodeName, false);
            if (stat == null) {
                return zk.create(nodeName, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String addNode(String nodeName, String data, List<ACL> acl, CreateMode createMode) {
        Stat stat;
        try {
            stat = zk.exists(nodeName, false);
            if (stat == null) {
                return zk.create(nodeName, data.getBytes(), acl, createMode);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void removeNode(String nodeName) {
        try {
            zk.delete(nodeName, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void removeNode(String nodeName, int version) {
        try {
            zk.delete(nodeName, version);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public String setData(String nodeName, String data) {
        Stat stat;
        try {
            stat = zk.exists(nodeName, false);
            if (stat != null) {
                zk.setData(nodeName, data.getBytes(), -1);
                return data;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 监控数据节点变化
     */
    public void monitorDataUpdate(String nodeName) {
        try {
            zk.getData(nodeName, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 节点的值有修改
                    if(event.getType() == EventType.NodeDataChanged) {
                        System.out.println(nodeName + "修改了值" + event.getPath());
                        // 触发一次就失效，所以需要递归注册
                        monitorDataUpdate(nodeName);
                    }
                }
            }, new Stat());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ZkUtils zkUtils = ZkUtils.getInstance("localhost", 2181);
        //zkUtils.removeNode("/test");
        
//        String result = zkUtils.addNode("/test");
//        System.out.println(result);
//
//        result = zkUtils.addNode("/test", "10");
//        System.out.println(result);
        String result = zkUtils.setData("/test", "hello");
        System.out.println(result);

        zkUtils.monitorDataUpdate("/test");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }

}
