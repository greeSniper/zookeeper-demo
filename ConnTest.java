package com.tangzhe.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 连接ZK测试
 */
public class ConnTest {

	public static void main(String[] args) {
		try {
			// 建立连接
			ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 1000 * 10, null);
			// 创建节点
			zooKeeper.create("/connTest", "connTest".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			byte[] result = zooKeeper.getData("/connTest", false, new Stat());
			System.out.println(new String(result));
			
			//zooKeeper.delete("/connTest", -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
